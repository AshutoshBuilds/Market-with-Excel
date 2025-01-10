"""Excel updater module."""
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import time
import threading
import queue
import pytz
import pythoncom
import win32com.client

logger = logging.getLogger(__name__)

class ExcelUpdater:
    def __init__(self):
        """Initialize Excel updater."""
        self._lock = threading.Lock()
        self._last_update = 0
        self.update_interval = 0.5  # Update every 500ms
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._excel_thread = threading.Thread(target=self._excel_worker, daemon=True)
        self._excel_thread.start()

    def _excel_worker(self):
        """Worker thread that handles all Excel operations."""
        try:
            # Initialize COM for Excel thread
            pythoncom.CoInitialize()
            
            # Create Excel instance
            excel = win32com.client.Dispatch("Excel.Application")
            excel.Visible = True
            
            # Create workbook
            wb = excel.Workbooks.Add()
            ws = wb.ActiveSheet
            
            # Set up headers
            ws.Cells(1, 1).Value = "Index"  # A1
            ws.Cells(1, 2).Value = "Spot"   # B1
            ws.Cells(1, 3).Value = "Change %" # C1
            
            # Set up index names
            indices = ["NIFTY 50", "NIFTY BANK", "NIFTY FIN SERVICE", "NIFTY MID SELECT", "SENSEX"]
            for i, index in enumerate(indices, start=2):
                ws.Cells(i, 1).Value = index
            
            # Format headers
            header_range = ws.Range("A1:C1")
            header_range.Font.Bold = True
            
            # Autofit columns
            ws.Columns("A:C").AutoFit()
            
            logger.info("Excel connection initialized successfully")
            
            while not self._stop_event.is_set():
                try:
                    # Get data from queue with timeout
                    try:
                        data = self._queue.get(timeout=0.1)
                    except queue.Empty:
                        continue
                    
                    market_data = data.get('market_data', {})
                    
                    # Fixed cell positions for each index
                    cells = {
                        "NIFTY 50": (2, 2),        # B2
                        "NIFTY BANK": (3, 2),      # B3
                        "NIFTY FIN SERVICE": (4, 2), # B4
                        "NIFTY MID SELECT": (5, 2), # B5
                        "SENSEX": (6, 2)           # B6
                    }
                    
                    # Update values
                    for index, (row, col) in cells.items():
                        if index in market_data:
                            data = market_data[index]
                            try:
                                # Get values
                                spot = data.get('last_price', 0)
                                change = data.get('change_percent', 0)
                                
                                # Update spot price in column B
                                ws.Cells(row, col).Value = spot
                                
                                # Update change % in column C
                                ws.Cells(row, col + 1).Value = change
                                
                                # Color the change cell based on value
                                if change >= 0:
                                    ws.Cells(row, col + 1).Font.Color = 0x008000  # Green
                                else:
                                    ws.Cells(row, col + 1).Font.Color = 0x0000FF  # Red
                                
                            except Exception as cell_error:
                                logger.error(f"Error updating {index}: {str(cell_error)}")
                                continue
                    
                    print(f"Excel updated at {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}")
                    
                except Exception as e:
                    logger.error(f"Error in Excel worker: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error initializing Excel: {str(e)}")
        finally:
            try:
                if 'wb' in locals():
                    wb.Save()
                if 'excel' in locals():
                    excel.Quit()
                pythoncom.CoUninitialize()
            except:
                pass

    def update_data(self, market_data: Dict[str, Dict[str, Any]], options_data: Dict[str, Dict[str, Any]]):
        """Queue market data update for Excel."""
        try:
            # Rate limit updates
            current_time = time.time()
            if current_time - self._last_update < self.update_interval:
                return
            self._last_update = current_time
            
            # Put data in queue for Excel thread
            self._queue.put({
                'market_data': market_data,
                'options_data': options_data
            })
            
        except Exception as e:
            logger.error(f"Error queueing data update: {str(e)}", exc_info=True)

    def __del__(self):
        """Cleanup when object is destroyed."""
        self._stop_event.set()
        if self._excel_thread.is_alive():
            self._excel_thread.join(timeout=5.0)  # Wait up to 5 seconds for thread to finish 