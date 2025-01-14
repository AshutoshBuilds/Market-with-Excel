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
from .greeks import GreeksCalculator

logger = logging.getLogger(__name__)

class ExcelUpdater:
    def __init__(self, update_interval=0.5):
        """Initialize Excel updater."""
        self._lock = threading.Lock()
        self._last_update = 0
        self.update_interval = update_interval
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._excel_thread = threading.Thread(target=self._excel_worker, daemon=True)
        self.options_rows = {}
        self.greeks_calculator = GreeksCalculator()
        self.current_date = datetime.now(pytz.timezone('Asia/Kolkata')).date()
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
            
            # Initialize current_date here so it's available throughout the method
            self.current_date = datetime.now(pytz.timezone('Asia/Kolkata')).date()
            
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
                'CE OI', 'CE Volume', 'CE LTP', 'CE Bid', 'CE Ask',  # CE Price columns (1-5)
                'CE Delta', 'CE Gamma', 'CE Theta', 'CE Vega', 'CE IV',  # CE Greeks columns (6-10)
                'Strike', 'Spot', 'Last Updated',  # Center columns (11-13)
                'PE Ask', 'PE Bid', 'PE LTP', 'PE Volume', 'PE OI',  # PE Price columns (14-18)
                'PE IV', 'PE Vega', 'PE Theta', 'PE Gamma', 'PE Delta',  # PE Greeks columns (19-23)
                'Expiry'  # Expiry date column (24)
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
                ws.Range(f"A{current_row}:X{current_row}").Merge()
                ws.Range(f"A{current_row}:X{current_row}").HorizontalAlignment = -4108  # Center
                ws.Range(f"A{current_row}:X{current_row}").Font.Bold = True
                ws.Range(f"A{current_row}:X{current_row}").Interior.Color = 0xE0E0E0  # Light gray background
                
                # Add options headers
                header_row = current_row + 1
                for col, header in enumerate(options_headers, start=1):
                    ws.Cells(header_row, col).Value = header
                    ws.Cells(header_row, col).Font.Bold = True
                    ws.Cells(header_row, col).Interior.Color = 0xF0F0F0  # Lighter gray for sub-headers
                
                # Format options headers
                header_range = ws.Range(f"A{header_row}:X{header_row}")
                header_range.Font.Bold = True
                header_range.HorizontalAlignment = -4108  # Center
                header_range.VerticalAlignment = -4108  # Center
                header_range.WrapText = True  # Enable text wrapping
                
                # Set column widths
                for col in range(1, 25):  # Columns A to X (including Expiry)
                    ws.Columns(col).ColumnWidth = 12  # Set width to 12 characters
                
                # Add borders to the entire options chain
                data_end_row = header_row + 11  # Header + 10 strikes + 1
                options_range = ws.Range(f"A{current_row}:X{data_end_row}")
                options_range.Borders.LineStyle = 1
                options_range.Borders.Weight = 2
                
                # Add borders between sections
                ce_section = ws.Range(f"A{header_row}:J{data_end_row}")  # CE section
                strike_section = ws.Range(f"K{header_row}:M{data_end_row}")  # Strike section
                pe_section = ws.Range(f"N{header_row}:W{data_end_row}")  # PE section
                expiry_section = ws.Range(f"X{header_row}:X{data_end_row}")  # Expiry section
                
                for section in [ce_section, strike_section, pe_section, expiry_section]:
                    section.Borders(9).LineStyle = 1  # xlEdgeLeft
                    section.Borders(10).LineStyle = 1  # xlEdgeRight
                    section.Borders(9).Weight = 2
                    section.Borders(10).Weight = 2
                
                # Store the starting row for this index's options data
                self.options_rows[index_display_names[index]] = header_row + 1
                
                # Update current_row for next index
                current_row = data_end_row + 2  # Add 2 for spacing between indices
            
            # Autofit all columns
            ws.Columns("A:X").AutoFit()
            
            # Center align all cells
            ws.Range(f"A1:X{current_row}").HorizontalAlignment = -4108
            
            logger.info("Excel connection initialized successfully")
            
            while not self._stop_event.is_set():
                try:
                    # Get data from queue with timeout
                    try:
                        data = self._queue.get(timeout=0.1)
                    except queue.Empty:
                        continue
                    
                    # Update current_date for each iteration
                    self.current_date = datetime.now(pytz.timezone('Asia/Kolkata')).date()
                    
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
                                    ws.Cells(row, 3).Interior.Color = 0x008000  # Green
                                else:
                                    ws.Cells(row, 3).Interior.Color = 0x0000FF  # Red
                                
                                # Color the spot cell based on comparison with previous close
                                if spot > close_price:
                                    ws.Cells(row, 2).Interior.Color = 0x008000  # Green
                                elif spot < close_price:
                                    ws.Cells(row, 2).Interior.Color = 0x0000FF  # Red
                                
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
                                    ws.Cells(row, 3).Interior.Color = 0x008000  # Green
                                    ws.Cells(row, 2).Interior.Color = 0x008000  # Green for LTP
                                else:
                                    ws.Cells(row, 3).Interior.Color = 0x0000FF  # Red
                                    ws.Cells(row, 2).Interior.Color = 0x0000FF  # Red for LTP
                                
                            except Exception as cell_error:
                                logger.error(f"Error updating futures {symbol}: {str(cell_error)}")
                                continue
                    
                    # Update options section
                    for index_name, start_row in self.options_rows.items():
                        # Get spot price for this index
                        spot_symbol = {
                            'NIFTY': 'NIFTY 50',
                            'BANKNIFTY': 'NIFTY BANK',
                            'FINNIFTY': 'NIFTY FIN SERVICE',
                            'MIDCPNIFTY': 'NIFTY MID SELECT',
                            'SENSEX': 'SENSEX'
                        }[index_name]
                        spot_price = market_data.get(spot_symbol, {}).get('last_price', 0)
                        print(f"Using spot price {spot_price} for {index_name} from {spot_symbol}")
                        
                        # Get all options for this index
                        index_options = {k: v for k, v in options_data.items() if k.startswith(index_name)}
                        
                        # Define strike gap for each index
                        strike_gap = {
                            'NIFTY': 50,
                            'BANKNIFTY': 100,
                            'FINNIFTY': 50,
                            'MIDCPNIFTY': 50,
                            'SENSEX': 100
                        }.get(index_name, 50)
                        
                        # Group by strike
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
                            pe_symbol = f"{index_name}_{strike}_PE"
                            ce_symbol = f"{index_name}_{strike}_CE"
                            
                            pe_data = options_data.get(pe_symbol, {})
                            ce_data = options_data.get(ce_symbol, {})
                            
                            if not pe_data and not ce_data:
                                continue
                            
                            # Calculate row for this strike
                            atm_strike = round(float(spot_price) / strike_gap) * strike_gap
                            row_offset = int((strike - (atm_strike - 5 * strike_gap)) / strike_gap)
                            row = self.options_rows[index_name] + row_offset
                            
                            try:
                                # Common updates first
                                ws.Cells(row, 11).Value = strike        # Strike (Column K)
                                ws.Cells(row, 12).Value = spot_price    # Spot (Column L)
                                ws.Cells(row, 13).Value = current_time  # Last Updated (Column M)
                                
                                # Initialize variables
                                ce_ltp = 0
                                pe_ltp = 0
                                ce_greeks = None
                                pe_greeks = None
                                
                                # Process CE data
                                if ce_data:
                                    ce_ltp = float(ce_data.get('last_price', 0))
                                    expiry_date = ce_data.get('expiry', datetime(2024, 1, 25).date())
                                    time_to_expiry = (expiry_date - self.current_date).days / 365.0
                                    ce_greeks = self._calculate_option_greeks(
                                        spot_price, strike, time_to_expiry, ce_ltp, is_call=True
                                    )
                                    
                                    # Update CE side values (Columns A-E)
                                    ws.Cells(row, 1).Value = ce_data.get('oi', 0)           # CE OI
                                    ws.Cells(row, 2).Value = ce_data.get('volume', 0)       # CE Volume
                                    ws.Cells(row, 3).Value = ce_ltp                         # CE LTP
                                    ws.Cells(row, 4).Value = ce_data.get('bid_price', 0)    # CE Bid
                                    ws.Cells(row, 5).Value = ce_data.get('ask_price', 0)    # CE Ask
                                    
                                    # Update CE Greeks (Columns F-J)
                                    if ce_greeks:
                                        ws.Cells(row, 6).Value = ce_greeks['delta']         # CE Delta
                                        ws.Cells(row, 7).Value = ce_greeks['gamma']         # CE Gamma
                                        ws.Cells(row, 8).Value = ce_greeks['theta']         # CE Theta
                                        ws.Cells(row, 9).Value = ce_greeks['vega']          # CE Vega
                                        ws.Cells(row, 10).Value = ce_greeks['iv']           # CE IV
                                
                                # Process PE data
                                if pe_data:
                                    pe_ltp = float(pe_data.get('last_price', 0))
                                    expiry_date = pe_data.get('expiry', datetime(2024, 1, 25).date())
                                    time_to_expiry = (expiry_date - self.current_date).days / 365.0
                                    pe_greeks = self._calculate_option_greeks(
                                        spot_price, strike, time_to_expiry, pe_ltp, is_call=False
                                    )
                                    
                                    # Update PE side values (Columns N-R)
                                    ws.Cells(row, 14).Value = pe_data.get('ask_price', 0)   # PE Ask
                                    ws.Cells(row, 15).Value = pe_data.get('bid_price', 0)   # PE Bid
                                    ws.Cells(row, 16).Value = pe_ltp                        # PE LTP
                                    ws.Cells(row, 17).Value = pe_data.get('volume', 0)      # PE Volume
                                    ws.Cells(row, 18).Value = pe_data.get('oi', 0)          # PE OI
                                    
                                    # Update PE Greeks (Columns S-W)
                                    if pe_greeks:
                                        ws.Cells(row, 19).Value = pe_greeks['iv']           # PE IV
                                        ws.Cells(row, 20).Value = pe_greeks['vega']         # PE Vega
                                        ws.Cells(row, 21).Value = pe_greeks['theta']        # PE Theta
                                        ws.Cells(row, 22).Value = pe_greeks['gamma']        # PE Gamma
                                        ws.Cells(row, 23).Value = pe_greeks['delta']        # PE Delta

                                # Format numbers
                                # Price and volume formatting (2 decimal places)
                                ws.Range(ws.Cells(row, 1), ws.Cells(row, 5)).NumberFormat = "0.00"      # CE prices/volumes
                                ws.Range(ws.Cells(row, 14), ws.Cells(row, 18)).NumberFormat = "0.00"    # PE prices/volumes
                                ws.Range(ws.Cells(row, 11), ws.Cells(row, 12)).NumberFormat = "0.00"    # Strike and Spot
                                
                                # Greeks formatting (4 decimal places)
                                ws.Range(ws.Cells(row, 6), ws.Cells(row, 9)).NumberFormat = "0.0000"    # CE Greeks
                                ws.Range(ws.Cells(row, 20), ws.Cells(row, 23)).NumberFormat = "0.0000"  # PE Greeks
                                
                                # IV formatting (percentage)
                                ws.Range(ws.Cells(row, 10), ws.Cells(row, 10)).NumberFormat = "0.00%"   # CE IV
                                ws.Range(ws.Cells(row, 19), ws.Cells(row, 19)).NumberFormat = "0.00%"   # PE IV

                                # Color coding for ITM/OTM/ATM
                                ce_range = ws.Range(ws.Cells(row, 1), ws.Cells(row, 10))   # CE side (columns A-J)
                                pe_range = ws.Range(ws.Cells(row, 14), ws.Cells(row, 23))  # PE side (columns N-W)
                                
                                # Check if this is the ATM strike
                                if strike == atm_strike:
                                    # ATM - Light yellow for both CE and PE sides
                                    ce_range.Interior.Color = 0xFFFF99  # Brighter yellow for ATM
                                    pe_range.Interior.Color = 0xFFFF99  # Brighter yellow for ATM
                                else:
                                    # For CE: ITM when strike < spot, OTM when strike > spot
                                    if strike < spot_price:  # ITM for CE
                                        ce_range.Interior.Color = 0xC6EFCE  # Light green
                                        pe_range.Interior.Color = 0xFFE4E1  # Light red for PE side (OTM)
                                    else:  # OTM for CE
                                        ce_range.Interior.Color = 0xFFE4E1  # Light red
                                        pe_range.Interior.Color = 0xC6EFCE  # Light green for PE side (ITM)

                                # Force immediate update
                                ws.Range(f"A{row}:W{row}").Value = ws.Range(f"A{row}:W{row}").Value
                                
                                # Update common columns
                                ws.Cells(row, 11).Value = strike        # Strike
                                ws.Cells(row, 12).Value = spot_price    # Spot
                                ws.Cells(row, 13).Value = current_time  # Last Updated
                                
                                # Get expiry date from either CE or PE data
                                expiry_date = None
                                if ce_data:
                                    expiry_date = ce_data.get('expiry')
                                elif pe_data:
                                    expiry_date = pe_data.get('expiry')
                                
                                if expiry_date:
                                    ws.Cells(row, 24).Value = expiry_date.strftime('%d-%b-%Y')  # Format: DD-MMM-YYYY

                            except Exception as cell_error:
                                logger.error(f"Error updating options for {index_name} strike {strike}: {str(cell_error)}")
                                continue
                    
                    # Update options data
                    if options_data:
                        for index_name in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX']:
                            # Get spot price for this index
                            spot_symbol = {
                                'NIFTY': 'NIFTY 50',
                                'BANKNIFTY': 'NIFTY BANK',
                                'FINNIFTY': 'NIFTY FIN SERVICE',
                                'MIDCPNIFTY': 'NIFTY MID SELECT',
                                'SENSEX': 'SENSEX'
                            }[index_name]
                            spot_price = market_data.get(spot_symbol, {}).get('last_price', 0)
                            
                            # Get all strikes for this index
                            strikes = sorted(set(float(symbol.split('_')[1]) 
                                              for symbol in options_data.keys() 
                                              if symbol.startswith(index_name)))
                            
                            if strikes:
                                # Find ATM strike (closest to spot price)
                                atm_strike = min(strikes, key=lambda x: abs(x - spot_price))
                    
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

    def _calculate_option_greeks(self, spot_price, strike_price, time_to_expiry, ltp, is_call):
        """Calculate Greeks for an option."""
        # First estimate IV using the market price
        iv = self.greeks_calculator.estimate_iv(
            S=spot_price,
            K=strike_price,
            T=time_to_expiry,  # This should be calculated based on expiry date
            market_price=ltp,
            is_call=is_call
        )
        
        # Then calculate all Greeks using the estimated IV
        greeks = self.greeks_calculator.calculate_greeks(
            S=spot_price,
            K=strike_price,
            T=time_to_expiry,
            sigma=iv/100,  # Convert back to decimal
            is_call=is_call
        )
        
        return greeks

    def _update_options_data(self, ws, options_data, market_data, current_time):
        """Update options chain data including Greeks."""
        try:
            # Update current_date for each call
            self.current_date = datetime.now(pytz.timezone('Asia/Kolkata')).date()
            expiry_date = datetime(2024, 1, 25, tzinfo=pytz.timezone('Asia/Kolkata')).date()
            time_to_expiry = (expiry_date - self.current_date).days / 365.0

            # Process each index
            for index_name, start_row in self.options_rows.items():
                try:
                    # Get spot price for this index
                    spot_symbol = {
                        'NIFTY': 'NIFTY 50',
                        'BANKNIFTY': 'NIFTY BANK',
                        'FINNIFTY': 'NIFTY FIN SERVICE',
                        'MIDCPNIFTY': 'NIFTY MID SELECT',
                        'SENSEX': 'SENSEX'
                    }[index_name]
                    
                    spot_price = float(market_data.get(spot_symbol, {}).get('last_price', 0))
                    if spot_price == 0:
                        continue

                    # Get strike gap for this index
                    strike_gap = {
                        'NIFTY': 50,
                        'BANKNIFTY': 100,
                        'FINNIFTY': 50,
                        'MIDCPNIFTY': 50,
                        'SENSEX': 100
                    }.get(index_name, 50)

                    # Find ATM strike
                    atm_strike = round(spot_price / strike_gap) * strike_gap
                    print(f"ATM strike for {index_name}: {atm_strike}")

                    # Get all options for this index
                    index_options = {k: v for k, v in options_data.items() if k.startswith(index_name)}
                    
                    # Group options by strike
                    strikes_data = {}
                    for symbol, data in index_options.items():
                        strike = data['strike']
                        option_type = data['option_type']
                        if strike not in strikes_data:
                            strikes_data[strike] = {'CE': None, 'PE': None}
                        strikes_data[strike][option_type] = data

                    # Process each strike
                    for strike in sorted(strikes_data.keys()):
                        try:
                            # Calculate row for this strike
                            row_offset = int((strike - (atm_strike - 5 * strike_gap)) / strike_gap)
                            row = start_row + row_offset

                            # Get CE and PE data
                            ce_data = strikes_data[strike].get('CE', {})
                            pe_data = strikes_data[strike].get('PE', {})

                            if not ce_data and not pe_data:
                                continue

                            # Update common columns
                            ws.Cells(row, 11).Value = strike        # Strike
                            ws.Cells(row, 12).Value = spot_price    # Spot
                            ws.Cells(row, 13).Value = current_time  # Last Updated

                            # Update CE data
                            if ce_data:
                                ce_ltp = float(ce_data.get('last_price', 0))
                                expiry_date = ce_data.get('expiry', datetime(2024, 1, 25).date())
                                time_to_expiry = (expiry_date - self.current_date).days / 365.0
                                
                                ws.Cells(row, 1).Value = ce_data.get('oi', 0)           # CE OI
                                ws.Cells(row, 2).Value = ce_data.get('volume', 0)       # CE Volume
                                ws.Cells(row, 3).Value = ce_ltp                         # CE LTP
                                ws.Cells(row, 4).Value = ce_data.get('bid_price', 0)    # CE Bid
                                ws.Cells(row, 5).Value = ce_data.get('ask_price', 0)    # CE Ask

                                # Calculate and update CE Greeks
                                ce_greeks = self._calculate_option_greeks(
                                    spot_price, strike, time_to_expiry, ce_ltp, is_call=True
                                )
                                if ce_greeks:
                                    ws.Cells(row, 6).Value = ce_greeks.get('delta', 0)  # CE Delta
                                    ws.Cells(row, 7).Value = ce_greeks.get('gamma', 0)  # CE Gamma
                                    ws.Cells(row, 8).Value = ce_greeks.get('theta', 0)  # CE Theta
                                    ws.Cells(row, 9).Value = ce_greeks.get('vega', 0)   # CE Vega
                                    ws.Cells(row, 10).Value = ce_greeks.get('iv', 0)    # CE IV

                            # Update PE data
                            if pe_data:
                                pe_ltp = float(pe_data.get('last_price', 0))
                                expiry_date = pe_data.get('expiry', datetime(2024, 1, 25).date())
                                time_to_expiry = (expiry_date - self.current_date).days / 365.0
                                
                                ws.Cells(row, 14).Value = pe_data.get('ask_price', 0)   # PE Ask
                                ws.Cells(row, 15).Value = pe_data.get('bid_price', 0)   # PE Bid
                                ws.Cells(row, 16).Value = pe_ltp                        # PE LTP
                                ws.Cells(row, 17).Value = pe_data.get('volume', 0)      # PE Volume
                                ws.Cells(row, 18).Value = pe_data.get('oi', 0)          # PE OI

                                # Calculate and update PE Greeks
                                pe_greeks = self._calculate_option_greeks(
                                    spot_price, strike, time_to_expiry, pe_ltp, is_call=False
                                )
                                if pe_greeks:
                                    ws.Cells(row, 19).Value = pe_greeks.get('iv', 0)    # PE IV
                                    ws.Cells(row, 20).Value = pe_greeks.get('vega', 0)  # PE Vega
                                    ws.Cells(row, 21).Value = pe_greeks.get('theta', 0) # PE Theta
                                    ws.Cells(row, 22).Value = pe_greeks.get('gamma', 0) # PE Gamma
                                    ws.Cells(row, 23).Value = pe_greeks.get('delta', 0) # PE Delta

                            # Apply formatting
                            ws.Range(ws.Cells(row, 1), ws.Cells(row, 5)).NumberFormat = "0.00"      # CE prices/volumes
                            ws.Range(ws.Cells(row, 14), ws.Cells(row, 18)).NumberFormat = "0.00"    # PE prices/volumes
                            ws.Range(ws.Cells(row, 11), ws.Cells(row, 12)).NumberFormat = "0.00"    # Strike and Spot
                            ws.Range(ws.Cells(row, 6), ws.Cells(row, 9)).NumberFormat = "0.0000"    # CE Greeks
                            ws.Range(ws.Cells(row, 20), ws.Cells(row, 23)).NumberFormat = "0.0000"  # PE Greeks
                            ws.Range(ws.Cells(row, 10), ws.Cells(row, 10)).NumberFormat = "0.00%"   # CE IV
                            ws.Range(ws.Cells(row, 19), ws.Cells(row, 19)).NumberFormat = "0.00%"   # PE IV

                            # Apply color coding
                            ce_range = ws.Range(ws.Cells(row, 1), ws.Cells(row, 10))   # CE side
                            pe_range = ws.Range(ws.Cells(row, 14), ws.Cells(row, 23))  # PE side

                            if strike == atm_strike:
                                ce_range.Interior.Color = 0xFFFF99  # ATM - yellow
                                pe_range.Interior.Color = 0xFFFF99
                            else:
                                if strike < spot_price:  # ITM for CE, OTM for PE
                                    ce_range.Interior.Color = 0xC6EFCE  # Light green
                                    pe_range.Interior.Color = 0xFFE4E1  # Light red
                                else:  # OTM for CE, ITM for PE
                                    ce_range.Interior.Color = 0xFFE4E1  # Light red
                                    pe_range.Interior.Color = 0xC6EFCE  # Light green

                            # Force immediate update
                            ws.Range(f"A{row}:W{row}").Value = ws.Range(f"A{row}:W{row}").Value

                        except Exception as cell_error:
                            logger.error(f"Error updating options for {index_name} strike {strike}: {str(cell_error)}")
                            continue

                except Exception as index_error:
                    logger.error(f"Error processing index {index_name}: {str(index_error)}")
                    continue

            logger.info(f"Options data updated at {current_time}")
                    
        except Exception as e:
            logger.error(f"Error in _update_options_data: {str(e)}")