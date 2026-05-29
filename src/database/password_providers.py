"""
Database password provider plugins

Allows swapping password retrieval mechanism without changing core code.
Supports: .pgpass file, AWS Secrets Manager, and environment variables.
"""

import os
import json
from pathlib import Path
from typing import Optional
from src.utils import get_logger

logger = get_logger(__name__)


def get_db_password() -> Optional[str]:
    """
    Get database password from configured provider

    Checks DB_PASSWORD_PROVIDER env var to determine which provider to use.
    Supported providers:
    - 'pgpass' (default - uses ~/.pgpass file, no password needed in code)
    - 'aws_secrets_manager' (for AWS RDS deployments)
    - 'env' (direct from environment variable, not recommended for production)

    Returns:
        Database password string, or None if using .pgpass

    Raises:
        ValueError: If password cannot be retrieved
    """
    provider = os.getenv("DB_PASSWORD_PROVIDER", "pgpass")

    logger.debug(f"Using password provider: {provider}")

    if provider == "pgpass":
        _get_password_from_pgpass()  # raises if .pgpass missing/misconfigured
        return None
    elif provider == "aws_secrets_manager":
        return _get_password_from_aws_secrets_manager()
    elif provider == "env":
        return _get_password_from_env()
    else:
        raise ValueError(f"Unknown password provider: {provider}")


def _get_password_from_pgpass() -> None:
    """
    Use .pgpass file for authentication (PostgreSQL standard)

    When using .pgpass, we don't pass a password to psycopg2.
    PostgreSQL client library automatically reads ~/.pgpass

    Returns:
        None (psycopg2 will read .pgpass automatically)

    Raises:
        ValueError: If .pgpass file doesn't exist or has wrong permissions
    """
    pgpass_path = Path.home() / ".pgpass"

    if not pgpass_path.exists():
        raise ValueError(
            f".pgpass file not found at {pgpass_path}\n"
            "Create it with: nano ~/.pgpass\n"
            "Format: hostname:port:database:username:password\n"
            "Then run: chmod 600 ~/.pgpass"
        )

    # Check permissions (must be 0600)
    pgpass_stat = pgpass_path.stat()
    pgpass_mode = oct(pgpass_stat.st_mode)[-3:]

    if pgpass_mode != "600":
        raise ValueError(
            f".pgpass file has incorrect permissions: {pgpass_mode}\n"
            f"PostgreSQL requires exactly 600 (read/write for owner only)\n"
            f"Fix with: chmod 600 {pgpass_path}"
        )

    logger.info(f"✅ Using .pgpass file for authentication: {pgpass_path}")

    # Return None - psycopg2 will automatically use .pgpass
    return None


def _get_password_from_aws_secrets_manager() -> str:
    """
    Retrieve database password from AWS Secrets Manager

    Returns:
        Database password

    Raises:
        ValueError: If secret cannot be retrieved
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        raise ImportError(
            "boto3 is required for AWS Secrets Manager. " "Install with: pip install boto3"
        )

    secret_name = os.getenv("DB_SECRET_NAME")
    region_name = os.getenv("AWS_REGION", "us-east-1")

    if not secret_name:
        raise ValueError("DB_SECRET_NAME environment variable is required for AWS Secrets Manager")

    logger.info(f"Fetching secret '{secret_name}' from AWS Secrets Manager")

    try:
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)

        # Retrieve the secret value
        response = client.get_secret_value(SecretId=secret_name)

        # Parse the secret
        if "SecretString" in response:
            secret = json.loads(response["SecretString"])

            # AWS RDS secrets typically have a 'password' key
            if "password" in secret:
                logger.info("✅ Successfully retrieved password from AWS Secrets Manager")
                return secret["password"]  # type: ignore[no-any-return]
            else:
                raise ValueError(
                    f"Secret '{secret_name}' does not contain 'password' key. "
                    f"Available keys: {list(secret.keys())}"
                )
        else:
            raise ValueError(f"Secret '{secret_name}' does not contain SecretString")

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "ResourceNotFoundException":
            raise ValueError(f"Secret '{secret_name}' not found in AWS Secrets Manager")
        elif error_code == "InvalidRequestException":
            raise ValueError(f"Invalid request to AWS Secrets Manager: {e}")
        elif error_code == "InvalidParameterException":
            raise ValueError(f"Invalid parameter: {e}")
        elif error_code == "DecryptionFailure":
            raise ValueError(f"Failed to decrypt secret: {e}")
        elif error_code == "InternalServiceError":
            raise ValueError(f"AWS Secrets Manager internal error: {e}")
        else:
            raise ValueError(f"Failed to retrieve secret: {e}")
    except Exception as e:
        logger.error(f"Unexpected error retrieving password: {e}", exc_info=True)
        raise ValueError(f"Failed to retrieve database password: {e}")


def _get_password_from_env() -> str:
    """
    Retrieve database password directly from environment variable

    Returns:
        Database password

    Raises:
        ValueError: If DB_PASSWORD not set
    """
    password = os.getenv("DB_PASSWORD")

    if not password:
        raise ValueError("DB_PASSWORD environment variable is required when using 'env' provider")

    logger.warning("⚠️  Using password from environment variable (not recommended for production)")
    return password


def find_pgpass_entry(
    host: Optional[str] = None,
    port: Optional[str] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    pgpass_path: Optional[Path] = None,
) -> Optional[dict]:
    """Return the first ``~/.pgpass`` entry matching the requested target.

    Each argument constrains the match only when non-``None``; a field of ``*``
    in the file matches anything (standard PostgreSQL .pgpass semantics). This
    lets callers that cannot rely on libpq's native .pgpass handling (notably
    asyncpg, which does not read .pgpass) pull the password for THEIR
    environment instead of blindly taking the first line — a multi-environment
    .pgpass otherwise risks authenticating against the wrong database.

    Returns ``{"host","port","database","user","password"}`` with the matched
    line's literal fields (``*`` preserved for the caller to resolve), or
    ``None`` when the file is absent or no line matches. The password may itself
    contain ``:`` (everything past the 4th colon). Backslash escapes are not
    interpreted (consistent with the prior hand-parser).
    """
    path = pgpass_path or (Path.home() / ".pgpass")
    if not path.exists():
        return None
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return None
    wanted = (("host", host), ("port", port), ("database", database), ("user", user))
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":")
        if len(parts) < 5:
            continue
        fields = {
            "host": parts[0],
            "port": parts[1],
            "database": parts[2],
            "user": parts[3],
            "password": ":".join(parts[4:]),
        }
        if all(want is None or fields[k] == "*" or fields[k] == want for k, want in wanted):
            return fields
    return None


def resolve_db_credentials() -> dict:
    """Resolve DB connection parameters for drivers that do not read .pgpass
    natively (e.g. asyncpg).

    The connection target (host/port/database/user) comes from the ``DB_*``
    environment variables. The password is taken from the ``~/.pgpass`` line
    that MATCHES that target — not the first line — so a multi-environment
    .pgpass cannot connect the caller to the wrong database; it falls back to
    ``DB_PASSWORD`` when no line matches. When the ``DB_*`` vars are unset the
    match is unconstrained, preserving the prior first-line behavior.

    Returns ``{"host","port","database","user","password"}`` (port as ``str``).
    """
    want_host = os.getenv("DB_HOST")
    want_port = os.getenv("DB_PORT")
    want_db = os.getenv("DB_NAME")
    want_user = os.getenv("DB_USER")
    entry = find_pgpass_entry(want_host, want_port, want_db, want_user)

    def _pick(want: Optional[str], field: Optional[str], default: str) -> str:
        if want:
            return want
        if field and field != "*":
            return field
        return default

    if entry is not None:
        return {
            "host": _pick(want_host, entry["host"], "localhost"),
            "port": _pick(want_port, entry["port"], "5432"),
            "database": _pick(want_db, entry["database"], "zerogex"),
            "user": _pick(want_user, entry["user"], "postgres"),
            "password": entry["password"],
        }
    return {
        "host": want_host or "localhost",
        "port": want_port or "5432",
        "database": want_db or "zerogex",
        "user": want_user or "postgres",
        "password": os.getenv("DB_PASSWORD", ""),
    }
