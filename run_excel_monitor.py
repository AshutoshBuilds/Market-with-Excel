"""Run Excel monitor."""
import logging
import os
import sys
import time
import signal
from pathlib import Path

# Add src directory to Python path
src_dir = str(Path(__file__).parent.parent.parent)
if src_dir not in sys.path:
    sys.path.append(src_dir)

from src.excel.updater import ExcelUpdater
from src.excel.websocket_handler import ExcelWebSocketHandler
from src.utils.auth import ensure_valid_tokens

# Set up basic logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
should_exit = False

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global should_exit
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    should_exit = True

def initialize_components(max_attempts=3):
    """Initialize WebSocket handler and Excel updater with retry logic."""
    attempt = 1
    while attempt <= max_attempts and not should_exit:
        try:
            logger.info(f"Initializing WebSocket handler (attempt {attempt}/{max_attempts})...")
            # Get API key and access token
            enctoken, access_token = ensure_valid_tokens()
            
            logger.info(f"Initializing Excel updater (attempt {attempt}/{max_attempts})...")
            excel_updater = ExcelUpdater()
            
            # Initialize WebSocket handler with Excel updater
            ws_handler = ExcelWebSocketHandler(excel_updater, access_token)
            
            return ws_handler
            
        except Exception as e:
            logger.error(f"Error during initialization (attempts left: {max_attempts-attempt}): {str(e)}")
            if attempt < max_attempts and not should_exit:
                time.sleep(2)
                attempt += 1
            else:
                raise Exception("Failed to initialize components after multiple attempts")

def run_monitor():
    """Run the Excel monitor."""
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    ws_handler = None
    try:
        # Initialize components
        ws_handler = initialize_components()
        
        # Keep the monitor running
        while not should_exit:
            time.sleep(0.5)  # Check every 500ms
            if ws_handler and not ws_handler.connected:
                logger.warning("WebSocket disconnected, attempting to reconnect...")
                ws_handler = initialize_components()
                
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Error in monitor: {str(e)}", exc_info=True)
    finally:
        logger.info("Shutting down gracefully...")
        if ws_handler:
            try:
                ws_handler.close()
            except Exception as e:
                logger.error(f"Error during shutdown: {str(e)}")
        sys.exit(0)

if __name__ == "__main__":
    run_monitor() 