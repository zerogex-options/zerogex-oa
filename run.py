#!/usr/bin/env python
"""
Main entry point for ZeroGEX components

Usage:
    python run.py <module> [args...]

Examples:
    python run.py auth
    python run.py client --test quote --symbol SPY
    python run.py client --test stream-bars --symbol SPY
    python run.py backfill --lookback-days 7    # Run backfill independently
    python run.py stream --underlying SPY       # Test streaming only
    python run.py ingest --underlying SPY       # Forward-only (no backfill)
    python run.py config
"""
import sys

def print_usage():
    """Print usage information"""
    print(__doc__)
    print("\nAvailable modules:")
    print("  auth          - Test TradeStation authentication")
    print("  client        - Test TradeStation API client (including stream-bars)")
    print("  backfill      - Run historical data backfill (INDEPENDENT)")
    print("  stream        - Test real-time options data streaming")
    print("  ingest        - Run main ingestion engine (FORWARD-ONLY)")
    print("  config        - Display current configuration")
    print("\nArchitecture:")
    print("  • main_engine (ingest) = Forward-only streaming")
    print("  • backfill_manager     = Independent historical data backfill")
    print("  • stream_manager       = Real-time data streaming (used by main_engine)")
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
            print("\n" + "="*80)
            print("RUNNING INDEPENDENT BACKFILL")
            print("="*80)
            print("Note: Backfill now runs independently and stores data directly.")
            print("      Use this to populate historical data as needed.")
            print("="*80 + "\n")
            from src.ingestion.backfill_manager import main
            main()

        elif module == "stream":
            print("\n" + "="*80)
            print("TESTING STREAM MANAGER")
            print("="*80)
            print("Note: This is a standalone test of the streaming component.")
            print("      For production streaming, use 'python run.py ingest'")
            print("="*80 + "\n")
            from src.ingestion.stream_manager import main
            main()

        elif module == "ingest":
            print("\n" + "="*80)
            print("RUNNING MAIN INGESTION ENGINE (FORWARD-ONLY)")
            print("="*80)
            print("Note: Main engine only streams forward-looking data.")
            print("      For historical backfill, run 'python run.py backfill'")
            print("="*80 + "\n")
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
