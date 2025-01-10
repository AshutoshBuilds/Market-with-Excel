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
            
            # Set up spot section headers
            spot_headers = [
                "Index",         # A1
                "Spot",          # B1
                "Change %",      # C1
                "Open",          # D1
                "High",          # E1
                "Low",           # F1
                "Close",         # G1
                "Last Updated"   # H1
            ]
            
            # Add spot headers
            for col, header in enumerate(spot_headers, start=1):
                ws.Cells(1, col).Value = header
            
            # Set up index names
            indices = ["NIFTY 50", "NIFTY BANK", "NIFTY FIN SERVICE", "NIFTY MID SELECT", "SENSEX"]
            for i, index in enumerate(indices, start=2):
                ws.Cells(i, 1).Value = index
            
            # Format spot headers
            spot_header_range = ws.Range(f"A1:H1")
            spot_header_range.Font.Bold = True
            
            # Add borders for spot section
            spot_data_range = ws.Range(f"A1:H{len(indices) + 1}")
            spot_data_range.Borders.LineStyle = 1
            spot_data_range.Borders.Weight = 2
            
            # Add a gap row
            gap_row = len(indices) + 3
            
            # Set up futures section headers
            futures_start_row = gap_row
            ws.Cells(futures_start_row, 1).Value = "FUTURES"
            ws.Range(f"A{futures_start_row}:N{futures_start_row}").Merge()
            ws.Range(f"A{futures_start_row}:N{futures_start_row}").HorizontalAlignment = -4108
            ws.Range(f"A{futures_start_row}:N{futures_start_row}").Font.Bold = True
            
            futures_headers = [
                "Symbol",        # A
                "LTP",          # B
                "Change %",     # C
                "Open",         # D
                "High",         # E
                "Low",          # F
                "Close",        # G
                "Volume",       # H
                "OI",          # I
                "Bid Price",    # J
                "Bid Qty",      # K
                "Ask Price",    # L
                "Ask Qty",      # M
                "Last Updated"  # N
            ]
            
            # Add futures headers
            futures_header_row = futures_start_row + 1
            for col, header in enumerate(futures_headers, start=1):
                ws.Cells(futures_header_row, col).Value = header
            
            # Format futures headers
            futures_header_range = ws.Range(f"A{futures_header_row}:N{futures_header_row}")
            futures_header_range.Font.Bold = True
            
            # Add borders for futures section
            futures_data_range = ws.Range(f"A{futures_start_row}:N{futures_header_row + len(indices)}")
            futures_data_range.Borders.LineStyle = 1
            futures_data_range.Borders.Weight = 2
            
            # Autofit all columns
            ws.Columns("A:N").AutoFit()
            
            # Center align all cells
            ws.Range(f"A1:N{futures_header_row + len(indices)}").HorizontalAlignment = -4108
            
            logger.info("Excel connection initialized successfully")
            
            while not self._stop_event.is_set():
                try:
                    # Get data from queue with timeout
                    try:
                        data = self._queue.get(timeout=0.1)
                    except queue.Empty:
                        continue
                    
                    market_data = data.get('market_data', {})
                    futures_data = data.get('futures_data', {})
                    options_data = data.get('options_data', {})
                    
                    # Update values
                    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
                    
                    # Update spot section
                    spot_cells = {
                        "NIFTY 50": 2,
                        "NIFTY BANK": 3,
                        "NIFTY FIN SERVICE": 4,
                        "NIFTY MID SELECT": 5,
                        "SENSEX": 6
                    }
                    
                    for index, row in spot_cells.items():
                        if index in market_data:
                            data = market_data[index]
                            try:
                                # Get all values directly from market data
                                spot = float(data.get('last_price', 0))
                                change = float(data.get('change_percent', 0))
                                
                                # Get OHLC values - using direct access since we know the fields exist
                                open_price = float(data['open'])
                                high_price = float(data['high'])
                                low_price = float(data['low'])
                                close_price = float(data['close'])
                                
                                # Update spot values
                                ws.Cells(row, 2).Value = spot           # Spot (B column)
                                ws.Cells(row, 3).Value = change         # Change % (C column)
                                ws.Cells(row, 4).Value = open_price     # Open (D column)
                                ws.Cells(row, 5).Value = high_price     # High (E column)
                                ws.Cells(row, 6).Value = low_price      # Low (F column)
                                ws.Cells(row, 7).Value = close_price    # Close (G column)
                                ws.Cells(row, 8).Value = current_time   # Last Updated (H column)
                                
                                # Force immediate update
                                ws.Range(f"B{row}:H{row}").Value = ws.Range(f"B{row}:H{row}").Value
                                
                                # Color the change cell based on value
                                if change >= 0:
                                    ws.Cells(row, 3).Font.Color = 0x008000  # Green
                                else:
                                    ws.Cells(row, 3).Font.Color = 0x0000FF  # Red
                                
                                # Color the spot cell based on comparison with previous close
                                if spot > close_price:
                                    ws.Cells(row, 2).Font.Color = 0x008000  # Green
                                elif spot < close_price:
                                    ws.Cells(row, 2).Font.Color = 0x0000FF  # Red
                                
                            except Exception as cell_error:
                                logger.error(f"Error updating {index}: {str(cell_error)}")
                                continue
                    
                    # Update futures section
                    futures_cells = {
                        "NIFTY FUT": futures_header_row + 1,
                        "BANKNIFTY FUT": futures_header_row + 2,
                        "FINNIFTY FUT": futures_header_row + 3,
                        "MIDCPNIFTY FUT": futures_header_row + 4,
                        "SENSEX FUT": futures_header_row + 5
                    }
                    
                    for symbol, row in futures_cells.items():
                        if symbol in futures_data:
                            data = futures_data[symbol]
                            try:
                                # Set symbol name
                                ws.Cells(row, 1).Value = symbol
                                
                                # Get futures values
                                ltp = float(data.get('last_price', 0))
                                change_percent = float(data.get('change_percent', 0))
                                open_price = float(data.get('open', 0))
                                high_price = float(data.get('high', 0))
                                low_price = float(data.get('low', 0))
                                close_price = float(data.get('close', 0))
                                volume = int(data.get('volume', 0))
                                oi = int(data.get('oi', 0))
                                bid_price = float(data.get('bid_price', 0))
                                bid_qty = int(data.get('bid_qty', 0))
                                ask_price = float(data.get('ask_price', 0))
                                ask_qty = int(data.get('ask_qty', 0))
                                
                                # Update futures values
                                ws.Cells(row, 2).Value = ltp            # LTP (B column)
                                ws.Cells(row, 3).Value = change_percent # Change % (C column)
                                ws.Cells(row, 4).Value = open_price     # Open (D column)
                                ws.Cells(row, 5).Value = high_price     # High (E column)
                                ws.Cells(row, 6).Value = low_price      # Low (F column)
                                ws.Cells(row, 7).Value = close_price    # Close (G column)
                                ws.Cells(row, 8).Value = volume         # Volume (H column)
                                ws.Cells(row, 9).Value = oi            # OI (I column)
                                ws.Cells(row, 10).Value = bid_price     # Bid Price (J column)
                                ws.Cells(row, 11).Value = bid_qty       # Bid Qty (K column)
                                ws.Cells(row, 12).Value = ask_price     # Ask Price (L column)
                                ws.Cells(row, 13).Value = ask_qty       # Ask Qty (M column)
                                ws.Cells(row, 14).Value = current_time  # Last Updated (N column)
                                
                                # Force immediate update
                                ws.Range(f"A{row}:N{row}").Value = ws.Range(f"A{row}:N{row}").Value
                                
                                # Color coding
                                if change_percent >= 0:
                                    ws.Cells(row, 3).Font.Color = 0x008000  # Green
                                    ws.Cells(row, 2).Font.Color = 0x008000  # Green for LTP
                                else:
                                    ws.Cells(row, 3).Font.Color = 0x0000FF  # Red
                                    ws.Cells(row, 2).Font.Color = 0x0000FF  # Red for LTP
                                
                            except Exception as cell_error:
                                logger.error(f"Error updating futures {symbol}: {str(cell_error)}")
                                continue
                    
                    print(f"Excel updated at {current_time}")
                    
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

    def update_data(self, market_data: Dict[str, Dict[str, Any]], futures_data: Dict[str, Dict[str, Any]], options_data: Dict[str, Dict[str, Any]]):
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
                'futures_data': futures_data,
                'options_data': options_data
            })
                    
        except Exception as e:
            logger.error(f"Error queueing data update: {str(e)}", exc_info=True)

    def __del__(self):
        """Cleanup when object is destroyed."""
        self._stop_event.set()
        if self._excel_thread.is_alive():
            self._excel_thread.join(timeout=5.0)  # Wait up to 5 seconds for thread to finish 