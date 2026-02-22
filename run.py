#!/usr/bin/env python
"""
Main entry point for ZeroGEX components

Usage:
    python run.py <module> [args...]

Examples:
    python run.py auth
    python run.py client --test quote --symbol SPY
    python run.py backfill --lookback-days 3
    python run.py stream --underlying SPY
    python run.py ingest --lookback-days 7
    python run.py config
"""
import sys

def print_usage():
    """Print usage information"""
    print(__doc__)
    print("\nAvailable modules:")
    print("  auth          - Test TradeStation authentication")
    print("  client        - Test TradeStation API client")
    print("  backfill      - Run historical data backfill")
    print("  stream        - Run real-time options data streaming")
    print("  ingest        - Run main ingestion engine (backfill + stream)")
    print("  config        - Display current configuration")
    print("\nFor module-specific help, run:")
    print("  python run.py <module> --help")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    module = sys.argv[1].lower()

    # Remove the module name from sys.argv so the module sees only its args
    sys.argv = [sys.argv[0]] + sys.argv[2:]

    try:
        if module == "auth":
            from src.ingestion.tradestation_auth import main
            main()

        elif module == "client":
            from src.ingestion.tradestation_client import main
            main()

        elif module == "backfill":
            from src.ingestion.backfill_manager import main
            main()

        elif module == "stream":
            from src.ingestion.stream_manager import main
            main()

        elif module == "ingest":
            from src.ingestion.main_engine import main
            main()

        elif module == "config":
            from src.config import print_config
            print_config()

        elif module in ["help", "-h", "--help"]:
            print_usage()

        else:
            print(f"❌ Unknown module: {module}")
            print_usage()
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(0)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
