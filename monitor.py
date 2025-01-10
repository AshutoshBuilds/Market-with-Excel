"""Excel monitor module."""
import logging
import time
from typing import Dict, Any
from src.utils.market import is_market_open, get_market_status
from src.excel.websocket_handler import ExcelWebSocketHandler
from src.excel.updater import ExcelUpdater
import json

logger = logging.getLogger(__name__)

def run_monitor(websocket_handler: ExcelWebSocketHandler, excel_updater: ExcelUpdater):
    """Run the Excel monitor."""
    try:
        logger.info("Starting Excel monitor...")
        
        # Initialize last update time
        last_update = 0
        update_interval = 0.1  # 100ms update interval
        reconnect_delay = 5  # seconds
        error_count = 0
        MAX_ERRORS = 3
        
        while True:
            try:
                # Check WebSocket connection
                if not websocket_handler.connected:
                    logger.warning("WebSocket disconnected, waiting for reconnection...")
                    time.sleep(reconnect_delay)
                    continue
                
                current_time = time.time()
                
                # Check if it's time to update
                if current_time - last_update >= update_interval:
                    try:
                        # Get latest data with thread safety
                        market_data = websocket_handler.get_market_data()
                        options_data = websocket_handler.get_options_data()
                        
                        # Log sample of the data we're about to write
                        if market_data:
                            sample_symbol = next(iter(market_data))
                            logger.debug(f"Market data sample for {sample_symbol}: {json.dumps(market_data[sample_symbol], indent=2)}")
                        
                        if options_data:
                            sample_symbol = next(iter(options_data))
                            sample_data = options_data[sample_symbol]
                            if sample_data:
                                sample_strike = next(iter(sample_data))
                                logger.debug(f"Options data sample for {sample_symbol} {sample_strike}: {json.dumps(sample_data[sample_strike], indent=2)}")
                        
                        # Update Excel with error handling
                        try:
                            excel_updater.update_data(market_data, options_data)
                            last_update = current_time
                            error_count = 0  # Reset error count on successful update
                        except Exception as e:
                            error_count += 1
                            logger.error(f"Error updating Excel (attempt {error_count}): {str(e)}")
                            if error_count >= MAX_ERRORS:
                                logger.error("Max Excel update errors reached, restarting monitor...")
                                raise Exception("Max Excel update errors reached")
                            time.sleep(1)  # Wait before retrying
                            
                    except Exception as e:
                        logger.error(f"Error getting market data: {str(e)}")
                        time.sleep(1)  # Wait before retrying
                    
                # Small sleep to prevent high CPU usage
                time.sleep(0.01)  # 10ms sleep
                
            except Exception as e:
                logger.error(f"Error in monitor loop: {str(e)}", exc_info=True)
                time.sleep(1)  # Wait before retrying
                
    except Exception as e:
        logger.error(f"Fatal error in monitor: {str(e)}", exc_info=True)
        raise
    finally:
        # Cleanup
        try:
            if websocket_handler:
                websocket_handler.close()
            logger.info("Monitor stopped, WebSocket closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}") 