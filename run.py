#!/usr/bin/env python
"""
Main entry point for ZeroGEX components
"""
import sys

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run.py <module>")
        print("\nAvailable modules:")
        print("  auth          - Test TradeStation authentication")
        print("  client        - Test TradeStation API client")
        print("  stream        - Run real-time options data streaming")
        print("  backfill      - Run historical data backfill")
        print("  ingest        - Run main ingestion engine (backfill + stream)")
        print("\nExample: python run.py auth")
        sys.exit(1)

    module = sys.argv[1]

    if module == "auth":
        from src.ingestion.tradestation_auth import main
        main()
    elif module == "client":
        from src.ingestion.tradestation_client import main
        main()
    elif module == "stream":
        from src.ingestion.stream_manager import main
        main()
    elif module == "backfill":
        from src.ingestion.backfill_manager import main
        main()
    elif module == "ingest":
        from src.ingestion.main_engine import main
        main()
    else:
        print(f"Unknown module: {module}")
        print("Run 'python run.py' for usage info")
        sys.exit(1)
