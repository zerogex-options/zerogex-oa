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
        print("  stream        - Run options data stream")
        print("\nExample: python run.py auth")
        sys.exit(1)
    
    module = sys.argv[1]
    
    if module == "auth":
        from src.ingestion.tradestation_auth import main
        main()
    elif module == "stream":
        # When you have your stream manager
        print("Stream module not yet implemented")
        # from src.ingestion.stream_manager import main
        # main()
    else:
        print(f"Unknown module: {module}")
        print("Run 'python run.py' for usage info")
        sys.exit(1)
