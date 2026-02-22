"""
ZeroGEX Main Ingestion Engine

Orchestrates the complete data ingestion pipeline:
1. Backfills historical data using BackfillManager
2. Streams real-time data using StreamManager
3. Coordinates database storage with 1-minute aggregation
"""

import os
import signal
import sys
from datetime import datetime
import pytz

from src.ingestion.tradestation_client import TradeStationClient
from src.ingestion.backfill_manager import BackfillManager
from src.ingestion.stream_manager import StreamManager
from src.utils import get_logger

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone('US/Eastern')


class MainEngine:
    """
    Main ingestion engine that orchestrates backfill and streaming
    
    Does NOT call TradeStationClient directly - delegates to:
    - BackfillManager for historical data
    - StreamManager for real-time data
    """

    def __init__(
        self,
        client: TradeStationClient,
        underlying: str = "SPY",
        num_expirations: int = 3,
        strike_distance: float = 10.0,
        lookback_days: int = 7,
        market_hours_poll_interval: int = 5,
        extended_hours_poll_interval: int = 30
    ):
        """
        Initialize main ingestion engine

        Args:
            client: TradeStationClient instance (passed to managers)
            underlying: Underlying symbol to track
            num_expirations: Number of expiration dates to track
            strike_distance: Strike distance from current price
            lookback_days: Days to backfill on startup
            market_hours_poll_interval: Poll interval during market hours (seconds)
            extended_hours_poll_interval: Poll interval during extended hours (seconds)
        """
        self.client = client
        self.underlying = underlying.upper()
        self.num_expirations = num_expirations
        self.strike_distance = strike_distance
        self.lookback_days = lookback_days
        self.market_hours_poll_interval = market_hours_poll_interval
        self.extended_hours_poll_interval = extended_hours_poll_interval
        
        self.running = False
        
        logger.info(f"Initialized MainEngine for {underlying}")
        logger.info(f"Config: {num_expirations} expirations, ±${strike_distance} strikes, "
                   f"{lookback_days} days lookback")
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"\n⚠️  Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def run_backfill(self):
        """
        Run backfill phase using BackfillManager
        
        Fetches historical data for the configured lookback period
        """
        logger.info("="*80)
        logger.info("BACKFILL PHASE")
        logger.info("="*80)
        logger.info(f"Backfilling {self.lookback_days} days of historical data...")
        
        # Create backfill manager - it handles all TradeStation API calls
        backfill = BackfillManager(
            client=self.client,
            underlying=self.underlying,
            num_expirations=self.num_expirations,
            strike_distance=self.strike_distance
        )
        
        # Run backfill in daily chunks to respect API limits
        for day_offset in range(self.lookback_days, 0, -1):
            logger.info(f"\nBackfilling day -{day_offset}...")
            
            try:
                # BackfillManager handles:
                # - Fetching underlying bars via TradeStationClient
                # - Fetching option expirations via TradeStationClient
                # - Fetching option strikes via TradeStationClient
                # - Fetching option quotes via TradeStationClient
                # - Storing data in database
                backfill.backfill(
                    lookback_days=1,  # 1 day at a time
                    interval=1,       # 1-minute bars
                    unit='Minute',
                    sample_every_n_bars=1  # Get every bar
                )
                
            except Exception as e:
                logger.error(f"Error backfilling day -{day_offset}: {e}", exc_info=True)
                # Continue with next day despite errors
        
        logger.info("\n✅ Backfill phase complete")
    
    def run_streaming(self):
        """
        Run streaming phase using StreamManager
        
        Streams real-time data with intelligent polling based on market hours
        """
        logger.info("="*80)
        logger.info("STREAMING PHASE")
        logger.info("="*80)
        
        # Create stream manager - it handles all TradeStation API calls
        stream_manager = StreamManager(
            client=self.client,
            underlying=self.underlying,
            num_expirations=self.num_expirations,
            strike_distance=self.strike_distance,
            poll_interval=self.market_hours_poll_interval  # Will be adjusted by manager
        )
        
        # Initialize the stream
        # StreamManager handles:
        # - Getting current price via TradeStationClient
        # - Getting option expirations via TradeStationClient
        # - Getting option strikes via TradeStationClient
        # - Building option symbol list
        if not stream_manager.initialize():
            logger.error("Failed to initialize streaming")
            return
        
        logger.info("✅ Streaming initialized")
        logger.info(f"Market hours poll interval: {self.market_hours_poll_interval}s")
        logger.info(f"Extended hours poll interval: {self.extended_hours_poll_interval}s")
        logger.info("Press Ctrl+C to stop\n")
        
        # Start streaming
        # StreamManager handles:
        # - Fetching underlying quotes via TradeStationClient
        # - Fetching option quotes via TradeStationClient
        # - Adjusting poll frequency based on market hours
        # - Storing data in database with 1-minute aggregation
        self.running = True
        
        try:
            stream_manager.stream(max_iterations=None)  # Run indefinitely
        except Exception as e:
            logger.error(f"Streaming error: {e}", exc_info=True)
        finally:
            logger.info("Streaming stopped")
    
    def run(self):
        """
        Run full ingestion pipeline: backfill → streaming
        
        This is the main entry point for the ingestion engine
        """
        logger.info("\n" + "="*80)
        logger.info("ZEROGEX MAIN INGESTION ENGINE")
        logger.info("="*80)
        logger.info(f"Underlying: {self.underlying}")
        logger.info(f"Expirations: {self.num_expirations}")
        logger.info(f"Strike Distance: ±${self.strike_distance}")
        logger.info(f"Lookback: {self.lookback_days} days")
        logger.info("="*80 + "\n")
        
        try:
            # Phase 1: Backfill historical data
            # Delegates to BackfillManager
            self.run_backfill()
            
            # Phase 2: Stream real-time data
            # Delegates to StreamManager
            self.run_streaming()
            
        except Exception as e:
            logger.error(f"Fatal error in main engine: {e}", exc_info=True)
            sys.exit(1)


def main():
    """Main entry point with argument parsing"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='ZeroGEX Main Ingestion Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Run with defaults
  python -m src.ingestion.main_engine
  
  # Custom configuration
  python -m src.ingestion.main_engine --underlying SPY --lookback-days 14
  
  # Debug mode
  python -m src.ingestion.main_engine --debug

Environment Variables (.env):
  See .env.example for complete configuration
  
  Key variables:
    INGEST_UNDERLYING=SPY
    INGEST_EXPIRATIONS=3
    INGEST_STRIKE_DISTANCE=10.0
    INGEST_LOOKBACK_DAYS=7
    INGEST_MARKET_HOURS_POLL=5
    INGEST_EXTENDED_HOURS_POLL=30
        '''
    )
    
    parser.add_argument('--underlying', type=str, 
                       help='Underlying symbol (env: INGEST_UNDERLYING)')
    parser.add_argument('--expirations', type=int, 
                       help='Number of expirations (env: INGEST_EXPIRATIONS)')
    parser.add_argument('--strike-distance', type=float, 
                       help='Strike distance (env: INGEST_STRIKE_DISTANCE)')
    parser.add_argument('--lookback-days', type=int, 
                       help='Days to backfill (env: INGEST_LOOKBACK_DAYS)')
    parser.add_argument('--debug', action='store_true', 
                       help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Load config from env with CLI override
    underlying = args.underlying or os.getenv('INGEST_UNDERLYING', 'SPY')
    num_expirations = args.expirations or int(os.getenv('INGEST_EXPIRATIONS', '3'))
    strike_distance = args.strike_distance or float(os.getenv('INGEST_STRIKE_DISTANCE', '10.0'))
    lookback_days = args.lookback_days or int(os.getenv('INGEST_LOOKBACK_DAYS', '7'))
    
    market_poll = int(os.getenv('INGEST_MARKET_HOURS_POLL', '5'))
    extended_poll = int(os.getenv('INGEST_EXTENDED_HOURS_POLL', '30'))
    
    # Set logging
    if args.debug or os.getenv('LOG_LEVEL', '').upper() == 'DEBUG':
        from src.utils import set_log_level
        set_log_level('DEBUG')
    
    # Initialize TradeStation client
    # This is the ONLY place we create the client
    # It gets passed to BackfillManager and StreamManager
    try:
        client = TradeStationClient(
            os.getenv('TRADESTATION_CLIENT_ID'),
            os.getenv('TRADESTATION_CLIENT_SECRET'),
            os.getenv('TRADESTATION_REFRESH_TOKEN'),
            sandbox=os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
        )
    except Exception as e:
        logger.error(f"Failed to initialize TradeStation client: {e}")
        sys.exit(1)
    
    # Create and run main engine
    # Engine orchestrates BackfillManager and StreamManager
    # Engine does NOT call TradeStationClient directly
    engine = MainEngine(
        client=client,
        underlying=underlying,
        num_expirations=num_expirations,
        strike_distance=strike_distance,
        lookback_days=lookback_days,
        market_hours_poll_interval=market_poll,
        extended_hours_poll_interval=extended_poll
    )
    
    engine.run()


if __name__ == '__main__':
    main()
