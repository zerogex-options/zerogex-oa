"""
Database password provider plugins

Allows swapping password retrieval mechanism without changing core code.
Currently supports AWS Secrets Manager, can be extended for other providers.
"""

import os
import json
from typing import Optional
from src.utils import get_logger

logger = get_logger(__name__)


def get_db_password() -> str:
    """
    Get database password from configured provider

    Checks DB_PASSWORD_PROVIDER env var to determine which provider to use.
    Supported providers:
    - 'aws_secrets_manager' (default for RDS)
    - 'env' (direct from environment variable)

    Returns:
        Database password string

    Raises:
        ValueError: If password cannot be retrieved
    """
    provider = os.getenv('DB_PASSWORD_PROVIDER', 'aws_secrets_manager')

    logger.debug(f"Using password provider: {provider}")

    if provider == 'aws_secrets_manager':
        return _get_password_from_aws_secrets_manager()
    elif provider == 'env':
        return _get_password_from_env()
    else:
        raise ValueError(f"Unknown password provider: {provider}")


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
            "boto3 is required for AWS Secrets Manager. "
            "Install with: pip install boto3"
        )

    secret_name = os.getenv('DB_SECRET_NAME')
    region_name = os.getenv('AWS_REGION', 'us-east-1')

    if not secret_name:
        raise ValueError(
            "DB_SECRET_NAME environment variable is required for AWS Secrets Manager"
        )

    logger.info(f"Fetching secret '{secret_name}' from AWS Secrets Manager")

    try:
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        # Retrieve the secret value
        response = client.get_secret_value(SecretId=secret_name)

        # Parse the secret
        if 'SecretString' in response:
            secret = json.loads(response['SecretString'])

            # AWS RDS secrets typically have a 'password' key
            if 'password' in secret:
                logger.info("✅ Successfully retrieved password from AWS Secrets Manager")
                return secret['password']
            else:
                raise ValueError(
                    f"Secret '{secret_name}' does not contain 'password' key. "
                    f"Available keys: {list(secret.keys())}"
                )
        else:
            raise ValueError(
                f"Secret '{secret_name}' does not contain SecretString"
            )

    except ClientError as e:
        error_code = e.response['Error']['Code']

        if error_code == 'ResourceNotFoundException':
            raise ValueError(f"Secret '{secret_name}' not found in AWS Secrets Manager")
        elif error_code == 'InvalidRequestException':
            raise ValueError(f"Invalid request to AWS Secrets Manager: {e}")
        elif error_code == 'InvalidParameterException':
            raise ValueError(f"Invalid parameter: {e}")
        elif error_code == 'DecryptionFailure':
            raise ValueError(f"Failed to decrypt secret: {e}")
        elif error_code == 'InternalServiceError':
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
    password = os.getenv('DB_PASSWORD')

    if not password:
        raise ValueError(
            "DB_PASSWORD environment variable is required when using 'env' provider"
        )

    logger.info("✅ Retrieved password from environment variable")
    return password


# Future providers can be added here, e.g.:
# def _get_password_from_hashicorp_vault() -> str:
#     """Retrieve password from HashiCorp Vault"""
#     pass
