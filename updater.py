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
            
            # Set up options chain sections
            options_start_row = futures_header_row + len(indices) + 2
            
            # Options headers
            options_headers = [
                "Strike",       # A
                "PE OI",        # B
                "PE Volume",    # C
                "PE LTP",       # D
                "PE Bid",       # E
                "PE Ask",       # F
                "Spot",         # G
                "CE Bid",       # H
                "CE Ask",       # I
                "CE LTP",       # J
                "CE Volume",    # K
                "CE OI",        # L
                "Last Updated"  # M
            ]
            
            # Create options chain tables for each index
            index_display_names = {
                "NIFTY 50": "NIFTY",
                "NIFTY BANK": "BANKNIFTY",
                "NIFTY FIN SERVICE": "FINNIFTY",
                "NIFTY MID SELECT": "MIDCPNIFTY",
                "SENSEX": "SENSEX"
            }
            
            current_row = options_start_row
            self.options_rows = {}  # Store row positions for each index's options
            
            for index in indices:
                # Add index name as header
                ws.Cells(current_row, 1).Value = f"{index_display_names[index]} OPTIONS"
                ws.Range(f"A{current_row}:M{current_row}").Merge()
                ws.Range(f"A{current_row}:M{current_row}").HorizontalAlignment = -4108
                ws.Range(f"A{current_row}:M{current_row}").Font.Bold = True
                
                # Add options headers
                header_row = current_row + 1
                for col, header in enumerate(options_headers, start=1):
                    ws.Cells(header_row, col).Value = header
                
                # Format options headers
                options_header_range = ws.Range(f"A{header_row}:M{header_row}")
                options_header_range.Font.Bold = True
                
                # Store the starting row for this index's options data
                self.options_rows[index_display_names[index]] = header_row + 1
                
                # Add space for 10 strikes (can be adjusted)
                data_end_row = header_row + 10
                
                # Add borders
                options_data_range = ws.Range(f"A{current_row}:M{data_end_row}")
                options_data_range.Borders.LineStyle = 1
                options_data_range.Borders.Weight = 2
                
                # Move to next section
                current_row = data_end_row + 2
            
            # Autofit all columns
            ws.Columns("A:N").AutoFit()
            
            # Center align all cells
            ws.Range(f"A1:N{current_row}").HorizontalAlignment = -4108
            
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
                    
                    # Update options section
                    for index_name, start_row in self.options_rows.items():
                        # Get options for this index
                        index_options = {k: v for k, v in options_data.items() if k.startswith(index_name)}
                        
                        # Group options by strike
                        strikes_data = {}
                        for symbol, data in index_options.items():
                            strike = data['strike']
                            option_type = data['option_type']
                            
                            if strike not in strikes_data:
                                strikes_data[strike] = {'PE': None, 'CE': None}
                            strikes_data[strike][option_type] = data
                        
                        # Sort strikes
                        sorted_strikes = sorted(strikes_data.keys())
                        
                        # Update each strike row
                        for i, strike in enumerate(sorted_strikes[:10]):  # Limit to 10 strikes
                            row = start_row + i
                            
                            # Get PE and CE data
                            pe_data = strikes_data[strike]['PE']
                            ce_data = strikes_data[strike]['CE']
                            
                            try:
                                # Update strike price
                                ws.Cells(row, 1).Value = strike
                                
                                # Update PE data if available
                                if pe_data:
                                    ws.Cells(row, 2).Value = pe_data.get('oi', 0)          # PE OI
                                    ws.Cells(row, 3).Value = pe_data.get('volume', 0)      # PE Volume
                                    ws.Cells(row, 4).Value = pe_data.get('last_price', 0)  # PE LTP
                                    ws.Cells(row, 5).Value = pe_data.get('bid_price', 0)   # PE Bid
                                    ws.Cells(row, 6).Value = pe_data.get('ask_price', 0)   # PE Ask
                                
                                # Update spot price in middle
                                spot_price = market_data.get(f"{index_name} 50" if index_name == "NIFTY" else 
                                                          f"NIFTY {index_name}" if index_name in ["BANK", "FIN SERVICE", "MID SELECT"] else 
                                                          index_name, {}).get('last_price', 0)
                                ws.Cells(row, 7).Value = spot_price
                                
                                # Update CE data if available
                                if ce_data:
                                    ws.Cells(row, 8).Value = ce_data.get('bid_price', 0)   # CE Bid
                                    ws.Cells(row, 9).Value = ce_data.get('ask_price', 0)   # CE Ask
                                    ws.Cells(row, 10).Value = ce_data.get('last_price', 0) # CE LTP
                                    ws.Cells(row, 11).Value = ce_data.get('volume', 0)     # CE Volume
                                    ws.Cells(row, 12).Value = ce_data.get('oi', 0)         # CE OI
                                
                                ws.Cells(row, 13).Value = current_time  # Last Updated
                                
                                # Force immediate update
                                ws.Range(f"A{row}:M{row}").Value = ws.Range(f"A{row}:M{row}").Value
                                
                                # Color coding for options
                                if pe_data:
                                    pe_change = pe_data.get('change_percent', 0)
                                    ws.Cells(row, 4).Font.Color = 0x008000 if pe_change >= 0 else 0x0000FF
                                
                                if ce_data:
                                    ce_change = ce_data.get('change_percent', 0)
                                    ws.Cells(row, 10).Font.Color = 0x008000 if ce_change >= 0 else 0x0000FF
                                
                            except Exception as cell_error:
                                logger.error(f"Error updating options for {index_name} strike {strike}: {str(cell_error)}")
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