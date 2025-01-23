"""Script to fetch historical data for Nifty 500 components."""
import sys
from pathlib import Path
import logging
from datetime import datetime, timedelta
import pytz
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import pickle
import pythoncom
import win32com.client
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

# Add src directory to Python path
src_dir = str(Path(__file__).parent.parent.parent)
if src_dir not in sys.path:
    sys.path.append(src_dir)

from src.utils.auth import ensure_valid_tokens
from src.excel.test_historical_data import fetch_historical_data

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get Excel file path
data_dir = Path(__file__).parent.parent.parent / 'data'
if not data_dir.exists():
    data_dir.mkdir(parents=True)

# Global variables
EXCEL_FILE = None
DEFAULT_EXCEL_FILE = str(data_dir / 'nifty500_historical_data.xlsx')

class RateLimiter:
    """Rate limiter for API requests."""
    def __init__(self, max_requests=3, time_window=1):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = threading.Lock()

    def wait_if_needed(self):
        """Wait if we've exceeded our rate limit."""
        with self.lock:
            now = time.time()
            # Remove old requests
            self.requests = [req_time for req_time in self.requests 
                           if now - req_time <= self.time_window]
            
            if len(self.requests) >= self.max_requests:
                # Wait until oldest request expires
                sleep_time = self.requests[0] + self.time_window - now
                if sleep_time > 0:
                    time.sleep(sleep_time)
                self.requests = self.requests[1:]
            
            # Add new request
            self.requests.append(now)

# Global rate limiter instance
rate_limiter = RateLimiter(max_requests=3, time_window=1)

def create_excel_template(excel):
    """Create Excel template with proper formatting."""
    global EXCEL_FILE
    EXCEL_FILE = DEFAULT_EXCEL_FILE
    
    try:
        # Check if file exists and is open
        try:
            if os.path.exists(EXCEL_FILE):
                with open(EXCEL_FILE, 'r+b'):
                    os.remove(EXCEL_FILE)
        except:
            logger.warning(f"Could not access {EXCEL_FILE}. It might be open in Excel.")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            EXCEL_FILE = str(data_dir / f'nifty500_historical_data_{timestamp}.xlsx')
            logger.info(f"Will try alternate location: {EXCEL_FILE}")

        # Create workbook
        wb = excel.Workbooks.Add()
        ws = wb.ActiveSheet
        
        # Set up headers with new timestamp columns
        headers = [
            'Symbol', 'Company Name', 'Industry',
            'Spot Price', 'Prev Close', 'Spot Change %', 'Spot Volume',
            'Future Price', 'Future Change %', 'Future Volume',
            'Spot-Future Spread %',
            '52W High', '52W High Date', '52W Low', '52W Low Date',
            '6M High', '6M High Date', '6M Low', '6M Low Date',
            '3M High', '3M High Date', '3M Low', '3M Low Date',
            '1M High', '1M High Date', '1M Low', '1M Low Date',
            '1W High', '1W High Time', '1W Low', '1W Low Time',
            '1D High', '1D High Time', '1D Low', '1D Low Time',
            '1H High', '1H High Time', '1H Low', '1H Low Time',
            '30M High', '30M High Time', '30M Low', '30M Low Time',
            '15M High', '15M High Time', '15M Low', '15M Low Time',
            'Avg Volume (1M)', 'Volume Ratio',
            'Price to 52W High %', 'Price to 52W Low %'
        ]
        
        try:
            # Add title and timestamp
            ws.Cells(1, 1).Value = "NIFTY 500 HISTORICAL DATA"
            ws.Cells(1, len(headers)-2).Value = f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Format title row
            last_col = len(headers)
            last_col_letter = chr(64 + last_col) if last_col <= 26 else chr(64 + (last_col//26)) + chr(64 + (last_col%26))
            
            # Title formatting
            title_range = ws.Range(f"A1:{chr(64 + len(headers)//2)}1")
            title_range.Merge()
            title_range.Font.Size = 14
            title_range.Font.Bold = True
            title_range.Interior.Color = 0x4F81BD  # Professional blue
            title_range.Font.Color = 0xFFFFFF  # White text
            title_range.HorizontalAlignment = -4108  # Center
            
            # Timestamp formatting
            timestamp_range = ws.Range(f"{chr(64 + len(headers)//2 + 1)}1:{last_col_letter}1")
            timestamp_range.Merge()
            timestamp_range.Font.Size = 11
            timestamp_range.Font.Bold = True
            timestamp_range.Interior.Color = 0x4F81BD  # Professional blue
            timestamp_range.Font.Color = 0xFFFFFF  # White text
            timestamp_range.HorizontalAlignment = -4108  # Center
            
            # Write and format column headers
            for col, header in enumerate(headers, start=1):
                ws.Cells(2, col).Value = header
            
            # Header row formatting
            header_range = ws.Range(f"A2:{last_col_letter}2")
            header_range.Font.Bold = True
            header_range.Interior.Color = 0xD9D9D9  # Light gray
            header_range.Borders.LineStyle = 1
            header_range.Borders.Weight = 2
            header_range.HorizontalAlignment = -4108  # Center
            header_range.VerticalAlignment = -4108  # Center
            header_range.WrapText = True
            
            # Set column widths and format
            for col in range(1, len(headers) + 1):
                # Set wider width for text columns
                if col in [1, 2, 3]:  # Symbol, Company Name, Industry
                    ws.Columns(col).ColumnWidth = 20
                elif 'Time' in headers[col-1] or 'Date' in headers[col-1]:
                    ws.Columns(col).ColumnWidth = 18  # Wider for timestamps
                else:
                    ws.Columns(col).ColumnWidth = 15
                
                # Format columns based on content type
                if col in [4, 5, 8, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28]:  # Price columns
                    ws.Columns(col).NumberFormat = "#,##0.00"
                elif col in [6, 9, 11, 31, 32]:  # Percentage columns
                    ws.Columns(col).NumberFormat = "0.00%"
                elif col in [7, 10, 29]:  # Volume columns
                    ws.Columns(col).NumberFormat = "#,##0"
            
            # Save workbook first
            wb.SaveAs(EXCEL_FILE)
            
            # Add a dummy row for AutoFilter
            for col in range(1, len(headers) + 1):
                ws.Cells(3, col).Value = "DUMMY"
            
            # Apply AutoFilter to header range including dummy row
            ws.Range(f"A2:{last_col_letter}3").AutoFilter()
            
            # Delete the dummy row
            ws.Rows(3).Delete()
            
            # Apply freeze panes after deleting dummy row
            ws.Range("A3").Select()
            excel.ActiveWindow.FreezePanes = True
            
            # Save again to preserve view settings
            wb.Save()
            logger.info(f"Created Excel template at {EXCEL_FILE}")
            
        except Exception as format_error:
            logger.error(f"Error formatting Excel template: {str(format_error)}")
            # Don't raise the error, continue with basic template
        
        # Return Excel objects even if formatting failed
        return wb, ws
        
    except Exception as e:
        logger.error(f"Error creating Excel template: {str(e)}")
        return None, None

def calculate_range_metrics(df, period_days):
    """Calculate high, low and range for a given period."""
    if df is None or len(df) == 0:
        return None, None, None
        
    period_data = df.tail(period_days)
    high = period_data['high'].max()
    low = period_data['low'].min()
    range_pct = ((high - low) / low * 100).round(2) if low > 0 else None
    return high, low, range_pct

def calculate_percentage_change(new_value, old_value):
    """Calculate percentage change with better validation."""
    try:
        if pd.isna(new_value) or pd.isna(old_value):
            return None
        if old_value == 0:
            return None
        # Return as percentage value (e.g., 80 for 80%)
        return ((new_value - old_value) / abs(old_value)).round(4)
    except:
        return None

def retry_excel_operation(operation, max_retries=5, retry_delay=30):
    """Retry an Excel operation with fixed delay."""
    for attempt in range(max_retries):
        try:
            result = operation()
            return result
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            logger.warning(f"Excel operation failed. Retrying in {retry_delay} seconds... Error: {str(e)}")
            time.sleep(retry_delay)

def update_excel_row(ws, symbol_row, row_data):
    """Update a single row in Excel with retry logic."""
    try:
        # Initialize COM for this thread if needed
        pythoncom.CoInitialize()
        
        def _update():
            for col, value in enumerate(row_data, start=1):
                ws.Cells(symbol_row, col).Value = value
            return True
            
        return retry_excel_operation(_update)
    finally:
        pythoncom.CoUninitialize()

def update_excel_data(symbol, spot_df, future_df, row, enctoken, spot_token):
    """Update Excel file with new data."""
    excel = None
    try:
        # Initialize COM for Excel thread
        pythoncom.CoInitialize()
        
        # Connect to Excel
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = True  # Make Excel visible
        excel.DisplayAlerts = False  # Disable alerts
        
        # Open workbook
        wb = excel.Workbooks.Open(EXCEL_FILE)
        ws = wb.ActiveSheet
        
        # Find next empty row starting from row 3
        next_row = 3
        while ws.Cells(next_row, 1).Value is not None:
            next_row += 1
        
        # Get intraday data
        intraday_data = fetch_with_retry(
            fetch_intraday_data,
            spot_token, enctoken,
            max_retries=3,
            base_delay=1
        ) if spot_df is not None and len(spot_df) > 0 else None
        
        # Calculate metrics
        if spot_df is not None and len(spot_df) > 0:
            spot_price = spot_df['close'].iloc[-1]
            spot_prev_close = spot_df['close'].iloc[-2] if len(spot_df) > 1 else spot_price
            spot_change = calculate_percentage_change(spot_price, spot_prev_close)
            spot_volume = spot_df['volume'].iloc[-1]  # Today's volume
            
            # Calculate range metrics
            year_high, year_high_date, year_low, year_low_date = get_high_low_with_time(
                spot_df.tail(252), 'year'
            )
            
            six_month_high, six_month_high_date, six_month_low, six_month_low_date = get_high_low_with_time(
                spot_df.tail(126), 'month'
            )
            
            three_month_high, three_month_high_date, three_month_low, three_month_low_date = get_high_low_with_time(
                spot_df.tail(63), 'month'
            )
            
            month_high, month_high_date, month_low, month_low_date = get_high_low_with_time(
                spot_df.tail(21), 'month'
            )
            
            week_high, week_high_time, week_low, week_low_time = get_high_low_with_time(
                spot_df.tail(5), 'week'
            )
            
            day_high, day_high_time, day_low, day_low_time = get_high_low_with_time(
                spot_df.tail(1), 'day'
            )
            
            # Calculate volume metrics with improved logic
            month_volume = spot_df['volume'].tail(21)  # Get last month's volume data
            volume_ratio = calculate_volume_ratio(spot_volume, month_volume)
            avg_volume_1m = int(month_volume.mean())  # Convert to integer
            
            # Calculate price to high/low percentages
            price_to_52w_high = calculate_percentage_change(spot_price, year_high)
            price_to_52w_low = calculate_percentage_change(spot_price, year_low)
        else:
            spot_price = spot_change = spot_volume = None
            year_high = year_low = None
            six_month_high = six_month_low = None
            three_month_high = three_month_low = None
            month_high = month_low = None
            week_high = week_low = None
            day_high = day_low = None
            avg_volume_1m = volume_ratio = None
            price_to_52w_high = price_to_52w_low = None
        
        # Calculate future metrics
        if future_df is not None and len(future_df) > 0:
            future_price = future_df['close'].iloc[-1]
            future_prev_close = future_df['close'].iloc[-2] if len(future_df) > 1 else future_price
            future_change = calculate_percentage_change(future_price, future_prev_close)
            future_volume = future_df['volume'].iloc[-1]
            spread = calculate_percentage_change(future_price, spot_price)
        else:
            future_price = future_change = future_volume = spread = None
        
        # Prepare row data
        row_data = [
            symbol,
            row['Company Name'],
            row['Industry'],
            spot_price,
            spot_prev_close,
            spot_change,
            spot_volume,
            future_price,
            future_change,
            future_volume,
            spread,
            year_high,
            year_high_date,
            year_low,
            year_low_date,
            six_month_high,
            six_month_high_date,
            six_month_low,
            six_month_low_date,
            three_month_high,
            three_month_high_date,
            three_month_low,
            three_month_low_date,
            month_high,
            month_high_date,
            month_low,
            month_low_date,
            week_high,
            week_high_time,
            week_low,
            week_low_time,
            day_high,
            day_high_time,
            day_low,
            day_low_time,
            intraday_data['hour']['high'] if intraday_data else None,
            intraday_data['hour']['high_time'] if intraday_data else None,
            intraday_data['hour']['low'] if intraday_data else None,
            intraday_data['hour']['low_time'] if intraday_data else None,
            intraday_data['thirty_min']['high'] if intraday_data else None,
            intraday_data['thirty_min']['high_time'] if intraday_data else None,
            intraday_data['thirty_min']['low'] if intraday_data else None,
            intraday_data['thirty_min']['low_time'] if intraday_data else None,
            intraday_data['fifteen_min']['high'] if intraday_data else None,
            intraday_data['fifteen_min']['high_time'] if intraday_data else None,
            intraday_data['fifteen_min']['low'] if intraday_data else None,
            intraday_data['fifteen_min']['low_time'] if intraday_data else None,
            avg_volume_1m,
            volume_ratio,
            price_to_52w_high,
            price_to_52w_low
        ]
        
        # Write data with retry logic
        success = update_excel_row(ws, next_row, row_data)
        if success:
            # Save periodically (every 10 stocks)
            if next_row % 10 == 0:
                retry_excel_operation(lambda: wb.Save())
            logger.info(f"Updated Excel with data for {symbol}")
        
    except Exception as e:
        logger.error(f"Error updating Excel data for {symbol}: {str(e)}")
        return False
    finally:
        if excel is not None:
            pythoncom.CoUninitialize()

def get_nearest_expiry_future(lookup_dict, symbol):
    """Get the nearest expiry future for a symbol."""
    try:
        futures = []
        for details in lookup_dict.values():
            if (details.get('tradingsymbol', '').startswith(symbol) and 
                details.get('exchange') == 'NFO' and 
                details.get('instrument_type') == 'FUT'):
                futures.append({
                    'token': str(details.get('instrument_token', '')),
                    'expiry': details.get('expiry', ''),
                    'tradingsymbol': details.get('tradingsymbol', '')
                })
        
        if not futures:
            return None
            
        # Sort by expiry date and get the nearest one
        futures.sort(key=lambda x: x['expiry'])
        return futures[0]
        
    except Exception as e:
        logger.error(f"Error getting nearest future for {symbol}: {str(e)}")
        return None

def load_instrument_tokens():
    """Load instrument tokens from pickle file."""
    try:
        # Get the latest instrument lookup file
        data_dir = Path(__file__).parent / 'data'
        lookup_files = list(data_dir.glob('instrument_lookup_*.pkl'))
        if not lookup_files:
            logger.error("No instrument lookup file found")
            return None
            
        latest_file = max(lookup_files)
        logger.info(f"Loading instrument tokens from {latest_file}")
        
        with open(latest_file, 'rb') as f:
            lookup_dict = pickle.load(f)
            
        # Convert to dictionary for faster lookup
        instrument_dict = {}
        for symbol, details in lookup_dict.items():
            # Skip if not NSE equity
            if details.get('exchange') != 'NSE' or details.get('instrument_type') != 'EQ':
                continue
                
            token = str(details.get('instrument_token', ''))
            if not token:
                continue
                
            # Get spot token
            instrument_dict[symbol] = {
                'spot': {'token': token},
                'future': None
            }
            
            # Get nearest future
            future = get_nearest_expiry_future(lookup_dict, symbol)
            if future:
                instrument_dict[symbol]['future'] = {
                    'token': future['token'],
                    'expiry': future['expiry'],
                    'tradingsymbol': future['tradingsymbol']
                }
                
        logger.info(f"Found {len(instrument_dict)} valid instruments")
        return instrument_dict, lookup_dict
        
    except Exception as e:
        logger.error(f"Error loading instrument tokens: {str(e)}")
        return None, None

def load_nifty500_components():
    """Load Nifty 500 components from CSV file."""
    try:
        csv_path = Path(__file__).parent / 'ind_nifty500list.csv'
        df = pd.read_csv(csv_path)
        return df
    except Exception as e:
        logger.error(f"Error loading Nifty 500 components: {str(e)}")
        return None

def fetch_intraday_data(token, enctoken):
    """Fetch intraday minute-level data for the last trading day."""
    try:
        # Get today's date in IST
        ist_tz = pytz.timezone('Asia/Kolkata')
        today = datetime.now(ist_tz)
        
        # If current time is after 3:30 PM, use today's data
        # If before 3:30 PM, use previous day's data
        target_date = today.date()
        if today.hour < 15 or (today.hour == 15 and today.minute < 30):
            target_date = target_date - timedelta(days=1)
        
        # Fetch minute-level data
        df = fetch_historical_data(token, target_date, target_date, 'minute', enctoken)
        
        if df is None or len(df) == 0:
            return None
            
        # Convert index to IST for time filtering
        df.index = df.index.tz_convert('Asia/Kolkata')
        
        # Get last hour data (2:30 PM to 3:30 PM)
        hour_mask = (
            ((df.index.hour == 14) & (df.index.minute >= 30)) |  # 2:30 PM to 2:59 PM
            ((df.index.hour == 15) & (df.index.minute <= 30))    # 3:00 PM to 3:30 PM
        )
        hour_data = df[hour_mask]
        
        # Get last 30 minutes data (3:00 PM to 3:30 PM)
        thirty_min_mask = (df.index.hour == 15) & (df.index.minute <= 30)
        thirty_min_data = df[thirty_min_mask]
        
        # Get last 15 minutes data (3:15 PM to 3:30 PM)
        fifteen_min_mask = (df.index.hour == 15) & (df.index.minute >= 15) & (df.index.minute <= 30)
        fifteen_min_data = df[fifteen_min_mask]
        
        # Find high/low values and their timestamps
        hour_high_idx = hour_data['high'].idxmax() if len(hour_data) > 0 else None
        hour_low_idx = hour_data['low'].idxmin() if len(hour_data) > 0 else None
        
        thirty_min_high_idx = thirty_min_data['high'].idxmax() if len(thirty_min_data) > 0 else None
        thirty_min_low_idx = thirty_min_data['low'].idxmin() if len(thirty_min_data) > 0 else None
        
        fifteen_min_high_idx = fifteen_min_data['high'].idxmax() if len(fifteen_min_data) > 0 else None
        fifteen_min_low_idx = fifteen_min_data['low'].idxmin() if len(fifteen_min_data) > 0 else None
        
        # Format timestamps and get values
        hour_high = hour_data.loc[hour_high_idx, 'high'] if hour_high_idx is not None else None
        hour_high_time = hour_high_idx.strftime('%H:%M') if hour_high_idx is not None else None
        hour_low = hour_data.loc[hour_low_idx, 'low'] if hour_low_idx is not None else None
        hour_low_time = hour_low_idx.strftime('%H:%M') if hour_low_idx is not None else None
        
        thirty_min_high = thirty_min_data.loc[thirty_min_high_idx, 'high'] if thirty_min_high_idx is not None else None
        thirty_min_high_time = thirty_min_high_idx.strftime('%H:%M') if thirty_min_high_idx is not None else None
        thirty_min_low = thirty_min_data.loc[thirty_min_low_idx, 'low'] if thirty_min_low_idx is not None else None
        thirty_min_low_time = thirty_min_low_idx.strftime('%H:%M') if thirty_min_low_idx is not None else None
        
        fifteen_min_high = fifteen_min_data.loc[fifteen_min_high_idx, 'high'] if fifteen_min_high_idx is not None else None
        fifteen_min_high_time = fifteen_min_high_idx.strftime('%H:%M') if fifteen_min_high_idx is not None else None
        fifteen_min_low = fifteen_min_data.loc[fifteen_min_low_idx, 'low'] if fifteen_min_low_idx is not None else None
        fifteen_min_low_time = fifteen_min_low_idx.strftime('%H:%M') if fifteen_min_low_idx is not None else None
        
        return {
            'hour': {
                'high': hour_high,
                'high_time': hour_high_time,
                'low': hour_low,
                'low_time': hour_low_time
            },
            'thirty_min': {
                'high': thirty_min_high,
                'high_time': thirty_min_high_time,
                'low': thirty_min_low,
                'low_time': thirty_min_low_time
            },
            'fifteen_min': {
                'high': fifteen_min_high,
                'high_time': fifteen_min_high_time,
                'low': fifteen_min_low,
                'low_time': fifteen_min_low_time
            }
        }
        
    except Exception as e:
        logger.error(f"Error fetching intraday data: {str(e)}")
        return None

def fetch_realtime_spot_data(token, enctoken):
    """Fetch real-time spot data."""
    try:
        # Get today's date in IST
        ist_tz = pytz.timezone('Asia/Kolkata')
        today = datetime.now(ist_tz)
        
        # Fetch minute-level data for today
        df = fetch_historical_data(token, today.date(), today.date(), 'minute', enctoken)
        
        if df is None or len(df) == 0:
            return None
            
        # Get latest price and volume
        latest_price = df['close'].iloc[-1]
        latest_volume = df['volume'].iloc[-1]
        
        # Get previous day's close
        yesterday = today - timedelta(days=1)
        prev_df = fetch_historical_data(token, yesterday, yesterday, 'day', enctoken)
        prev_close = prev_df['close'].iloc[-1] if prev_df is not None and len(prev_df) > 0 else None
        
        return {
            'price': latest_price,
            'volume': latest_volume,
            'prev_close': prev_close
        }
        
    except Exception as e:
        logger.error(f"Error fetching real-time spot data: {str(e)}")
        return None

def update_spot_data(ws, symbol_row, spot_data):
    """Update spot price and change % in real-time."""
    try:
        if spot_data is None:
            return False
            
        price = spot_data['price']
        volume = spot_data['volume']
        prev_close = spot_data['prev_close']
        
        if price is not None and prev_close is not None:
            change_pct = calculate_percentage_change(price, prev_close)
            
            # Update spot columns
            ws.Cells(symbol_row, 4).Value = price  # Spot Price
            ws.Cells(symbol_row, 5).Value = prev_close  # Prev Close
            ws.Cells(symbol_row, 6).Value = change_pct  # Spot Change % (now returns decimal)
            ws.Cells(symbol_row, 7).Value = volume  # Spot Volume
            
            return True
    except Exception as e:
        logger.error(f"Error updating spot data: {str(e)}")
        return False

def fetch_realtime_future_data(token, enctoken):
    """Fetch real-time future data."""
    try:
        # Get today's date in IST
        ist_tz = pytz.timezone('Asia/Kolkata')
        today = datetime.now(ist_tz).date()
        
        # Fetch minute-level data for today
        df = fetch_historical_data(token, today, today, 'minute', enctoken)
        
        if df is None or len(df) == 0:
            return None
            
        # Get latest price and volume
        latest_price = df['close'].iloc[-1]
        latest_volume = df['volume'].iloc[-1]
        
        # Get previous day's close
        yesterday = today - timedelta(days=1)
        prev_df = fetch_historical_data(token, yesterday, yesterday, 'day', enctoken)
        prev_close = prev_df['close'].iloc[-1] if prev_df is not None and len(prev_df) > 0 else None
        
        return {
            'price': latest_price,
            'volume': latest_volume,
            'prev_close': prev_close
        }
        
    except Exception as e:
        logger.error(f"Error fetching real-time future data: {str(e)}")
        return None

def fetch_with_retry(fetch_func, *args, max_retries=3, base_delay=30):
    """Fetch data with retry logic and rate limiting."""
    for retry in range(max_retries):
        try:
            # Wait for rate limit
            rate_limiter.wait_if_needed()
            
            # Attempt the fetch
            result = fetch_func(*args)
            
            # If successful, return the result
            if result is not None:
                return result
            
            # If result is None but no exception, retry with fixed delay
            logger.warning(f"Fetch returned None, retrying in {base_delay} seconds...")
            time.sleep(base_delay)
            
        except Exception as e:
            if "Too many requests" in str(e):
                logger.warning(f"Rate limit hit, retrying in {base_delay} seconds...")
                time.sleep(base_delay)
                continue
            else:
                logger.error(f"Error in fetch: {str(e)}")
                if retry < max_retries - 1:
                    logger.warning(f"Retrying in {base_delay} seconds...")
                    time.sleep(base_delay)
                    continue
                return None
    return None

def calculate_volume_ratio(current_volume, volume_series):
    """Calculate volume ratio with proper validation."""
    try:
        if pd.isna(current_volume) or volume_series is None or len(volume_series) == 0:
            return None
            
        # Calculate 1-month average volume (excluding today)
        avg_volume = volume_series.iloc[:-1].mean()  # Exclude today's volume from average
        
        if avg_volume == 0 or pd.isna(avg_volume):
            return None
            
        # Calculate ratio as percentage
        ratio = (current_volume / avg_volume * 100)
        
        # Return as percentage value
        return ratio.round(2)
    except:
        return None

def get_high_low_with_time(df, period_type='day', lookback=None):
    """Get high/low values with their timestamps using most granular data available."""
    if df is None or len(df) == 0:
        return None, None, None, None
        
    if lookback:
        df = df.tail(lookback)
    
    # Find high and low with their timestamps
    high_idx = df['high'].idxmax()
    low_idx = df['low'].idxmin()
    
    high = df.loc[high_idx, 'high']
    low = df.loc[low_idx, 'low']
    
    # Format timestamp consistently for all periods
    # If time is not available (daily data), use 00:00
    try:
        high_time = high_idx.strftime('%Y-%m-%d %H:%M')
    except:
        high_time = high_idx.strftime('%Y-%m-%d') + ' 00:00'
        
    try:
        low_time = low_idx.strftime('%Y-%m-%d %H:%M')
    except:
        low_time = low_idx.strftime('%Y-%m-%d') + ' 00:00'
    
    return high, high_time, low, low_time

def fetch_instrument_data(symbol, tokens, enctoken, from_date, to_date, interval):
    """Fetch all data for a single instrument with rate limiting."""
    try:
        # Convert datetime objects to date objects at the start
        if isinstance(from_date, datetime):
            from_date = from_date.date()
        if isinstance(to_date, datetime):
            to_date = to_date.date()
            
        spot_df = future_df = spot_data = future_data = intraday_data = None
        
        # Fetch spot data
        if tokens['spot'] is not None:
            spot_token = tokens['spot']['token']
            
            # Initialize empty DataFrame for spot data
            spot_df = pd.DataFrame()
            
            # Calculate date ranges based on API limits
            today = datetime.now(pytz.timezone('Asia/Kolkata')).date()
            
            date_ranges = [
                # Last 60 days: minute data
                (max(from_date, today - timedelta(days=60)), today, 'minute'),
                # 60-100 days ago: 5-minute data
                (max(from_date, today - timedelta(days=100)), today - timedelta(days=60), '5minute'),
                # 100-200 days ago: 15-minute data
                (max(from_date, today - timedelta(days=200)), today - timedelta(days=100), '15minute'),
                # Beyond 200 days: day data
                (from_date, today - timedelta(days=200), 'day')
            ]
            
            # Fetch data for each range
            for start_date, end_date, interval in date_ranges:
                if start_date >= end_date:
                    continue
                    
                logger.info(f"Fetching {interval} data for {symbol} from {start_date} to {end_date}")
                temp_df = fetch_with_retry(
                    fetch_historical_data,
                    spot_token, start_date, end_date, interval, enctoken,
                    max_retries=3,
                    base_delay=2
                )
                
                if temp_df is not None and len(temp_df) > 0:
                    spot_df = pd.concat([spot_df, temp_df])
            
            if len(spot_df) == 0:
                logger.warning(f"Could not fetch any historical data for {symbol}")
                return None
            
            # Remove duplicates and sort index
            spot_df = spot_df[~spot_df.index.duplicated(keep='first')]
            spot_df.sort_index(inplace=True)
            
            # Get today's minute data for current values
            today = datetime.now(pytz.timezone('Asia/Kolkata')).date()
            yesterday = today - timedelta(days=1)
            
            # Get yesterday's data for previous close
            prev_day_df = fetch_with_retry(
                fetch_historical_data,
                spot_token, yesterday, yesterday, 'day', enctoken,
                max_retries=3,
                base_delay=1
            )
            
            # Get today's minute data
            spot_minute_df = fetch_with_retry(
                fetch_historical_data,
                spot_token, today, today, 'minute', enctoken,
                max_retries=3,
                base_delay=1
            )
            
            if spot_df is not None and spot_minute_df is not None and prev_day_df is not None:
                # Get previous day's close
                prev_close = prev_day_df['close'].iloc[-1] if len(prev_day_df) > 0 else None
                
                # Update today's data with minute-level granularity
                latest_data = spot_minute_df.iloc[-1]
                spot_df.loc[today] = latest_data
                
                # Store previous close separately
                spot_data = {
                    'price': latest_data['close'],
                    'volume': latest_data['volume'],
                    'prev_close': prev_close
                }
                
                # Fetch intraday data with retry
                intraday_data = fetch_with_retry(
                    fetch_intraday_data,
                    spot_token, enctoken,
                    max_retries=3,
                    base_delay=1
                )
        
        # Similar changes for future data
        if tokens['future'] is not None:
            future_token = tokens['future']['token']
            
            # Initialize empty DataFrame for future data
            future_df = pd.DataFrame()
            
            # Fetch data for each range
            for start_date, end_date, interval in date_ranges:
                if start_date >= end_date:
                    continue
                    
                logger.info(f"Fetching {interval} future data for {symbol} from {start_date} to {end_date}")
                temp_df = fetch_with_retry(
                    fetch_historical_data,
                    future_token, start_date, end_date, interval, enctoken,
                    max_retries=3,
                    base_delay=2
                )
                
                if temp_df is not None and len(temp_df) > 0:
                    future_df = pd.concat([future_df, temp_df])
            
            if len(future_df) > 0:
                # Remove duplicates and sort index
                future_df = future_df[~future_df.index.duplicated(keep='first')]
                future_df.sort_index(inplace=True)
                
                # Get today's minute data for futures
                future_minute_df = fetch_with_retry(
                    fetch_historical_data,
                    future_token, today, today, 'minute', enctoken,
                    max_retries=3,
                    base_delay=1
                )
                
                if future_minute_df is not None and len(future_minute_df) > 0:
                    future_df.loc[today] = future_minute_df.iloc[-1]
        
        return {
            'symbol': symbol,
            'spot_df': spot_df,
            'future_df': future_df,
            'spot_data': spot_data,
            'future_data': future_data,
            'intraday_data': intraday_data
        }
        
    except Exception as e:
        logger.error(f"Error in fetch_instrument_data for {symbol}: {str(e)}")
        return None

def fetch_nifty500_historical_data(lookback_days=365, interval='day'):
    """Fetch historical data for all Nifty 500 components."""
    excel = None
    try:
        # Get authentication tokens
        enctoken, _ = ensure_valid_tokens()
        logger.info("Starting Nifty 500 historical data fetch")
        
        # Load Nifty 500 components and instrument tokens
        components_df = load_nifty500_components()
        instrument_dict, lookup_dict = load_instrument_tokens()
        
        if components_df is None or instrument_dict is None:
            return
            
        # Log first and last instruments
        total_instruments = len(components_df)
        logger.info(f"Total instruments to process: {total_instruments}")
        
        # Set date range
        to_date = datetime.now(pytz.timezone('Asia/Kolkata'))
        from_date = to_date - timedelta(days=lookback_days)
        
        # Initialize COM for Excel thread
        pythoncom.CoInitialize()
        
        # Create Excel instance
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = True
        excel.DisplayAlerts = False
        
        # Create Excel file and get workbook/worksheet
        wb, ws = create_excel_template(excel)
        if wb is None or ws is None:
            return

        # Process one instrument at a time with progress bar
        total_processed = 0
        with tqdm(total=total_instruments, desc="Processing instruments") as pbar:
            for idx, row in components_df.iterrows():
                symbol = row['Symbol']
                tokens = instrument_dict.get(symbol)
                
                # Clear log of previous instrument's DataFrame info
                logger.info(f"Processing [{idx+1}/{total_instruments}] {symbol}")
                
                if tokens is None:
                    logger.warning(f"No tokens found for {symbol}")
                    pbar.update(1)
                    continue
                
                try:
                    # Fetch data for this instrument
                    data = fetch_instrument_data(
                        symbol,
                        tokens,
                        enctoken,
                        from_date,
                        to_date,
                        interval
                    )
                    
                    if data is not None:
                        # Calculate next row (3 is the first data row)
                        next_row = total_processed + 3
                        
                        # Calculate metrics and update Excel
                        spot_df = data['spot_df']
                        future_df = data['future_df']
                        spot_data = data['spot_data']
                        
                        if spot_df is not None and len(spot_df) > 0 and spot_data is not None:
                            spot_price = spot_data['price']
                            spot_prev_close = spot_data['prev_close']
                            spot_change = calculate_percentage_change(spot_price, spot_prev_close)
                            spot_volume = spot_data['volume']
                            
                            # Calculate range metrics with timestamps
                            year_high, year_high_date, year_low, year_low_date = get_high_low_with_time(
                                spot_df.tail(252), 'year'
                            )
                            
                            six_month_high, six_month_high_date, six_month_low, six_month_low_date = get_high_low_with_time(
                                spot_df.tail(126), 'month'
                            )
                            
                            three_month_high, three_month_high_date, three_month_low, three_month_low_date = get_high_low_with_time(
                                spot_df.tail(63), 'month'
                            )
                            
                            month_high, month_high_date, month_low, month_low_date = get_high_low_with_time(
                                spot_df.tail(21), 'month'
                            )
                            
                            week_high, week_high_time, week_low, week_low_time = get_high_low_with_time(
                                spot_df.tail(5), 'week'
                            )
                            
                            day_high, day_high_time, day_low, day_low_time = get_high_low_with_time(
                                spot_df.tail(1), 'day'
                            )
                            
                            # Get intraday data with timestamps
                            intraday = data.get('intraday_data', {})
                            if intraday:
                                hour_high = intraday.get('hour', {}).get('high')
                                hour_high_time = intraday.get('hour', {}).get('high_time')
                                hour_low = intraday.get('hour', {}).get('low')
                                hour_low_time = intraday.get('hour', {}).get('low_time')
                                
                                thirty_min_high = intraday.get('thirty_min', {}).get('high')
                                thirty_min_high_time = intraday.get('thirty_min', {}).get('high_time')
                                thirty_min_low = intraday.get('thirty_min', {}).get('low')
                                thirty_min_low_time = intraday.get('thirty_min', {}).get('low_time')
                                
                                fifteen_min_high = intraday.get('fifteen_min', {}).get('high')
                                fifteen_min_high_time = intraday.get('fifteen_min', {}).get('high_time')
                                fifteen_min_low = intraday.get('fifteen_min', {}).get('low')
                                fifteen_min_low_time = intraday.get('fifteen_min', {}).get('low_time')
                            else:
                                hour_high = hour_high_time = hour_low = hour_low_time = None
                                thirty_min_high = thirty_min_high_time = thirty_min_low = thirty_min_low_time = None
                                fifteen_min_high = fifteen_min_high_time = fifteen_min_low = fifteen_min_low_time = None
                            
                            # Calculate volume metrics with improved logic
                            month_volume = spot_df['volume'].tail(21)  # Get last month's volume data
                            volume_ratio = calculate_volume_ratio(spot_volume, month_volume)
                            avg_volume_1m = int(month_volume.mean())  # Convert to integer
                            
                            # Calculate price to high/low percentages consistently
                            price_to_52w_high = calculate_percentage_change(spot_price, year_high)
                            price_to_52w_low = calculate_percentage_change(spot_price, year_low)  
                        else:
                            spot_price = spot_change = spot_volume = None
                            year_high = year_low = None
                            six_month_high = six_month_low = None
                            three_month_high = three_month_low = None
                            month_high = month_low = None
                            week_high = week_low = None
                            day_high = day_low = None
                            avg_volume_1m = volume_ratio = None
                            price_to_52w_high = price_to_52w_low = None
                        
                        # Calculate future metrics
                        if future_df is not None and len(future_df) > 0:
                            future_price = future_df['close'].iloc[-1]
                            future_prev_close = future_df['close'].iloc[-2] if len(future_df) > 1 else future_price
                            future_change = calculate_percentage_change(future_price, future_prev_close)
                            future_volume = future_df['volume'].iloc[-1]
                            spread = calculate_percentage_change(future_price, spot_price)
                        else:
                            future_price = future_change = future_volume = spread = None
                        
                        # Write data to Excel with timestamps
                        ws.Cells(next_row, 1).Value = symbol
                        ws.Cells(next_row, 2).Value = row['Company Name']
                        ws.Cells(next_row, 3).Value = row['Industry']
                        ws.Cells(next_row, 4).Value = spot_price
                        ws.Cells(next_row, 5).Value = spot_prev_close
                        ws.Cells(next_row, 6).Value = spot_change
                        ws.Cells(next_row, 7).Value = spot_volume
                        ws.Cells(next_row, 8).Value = future_price
                        ws.Cells(next_row, 9).Value = future_change
                        ws.Cells(next_row, 10).Value = future_volume
                        ws.Cells(next_row, 11).Value = spread
                        ws.Cells(next_row, 12).Value = year_high
                        ws.Cells(next_row, 13).Value = year_high_date
                        ws.Cells(next_row, 14).Value = year_low
                        ws.Cells(next_row, 15).Value = year_low_date
                        ws.Cells(next_row, 16).Value = six_month_high
                        ws.Cells(next_row, 17).Value = six_month_high_date
                        ws.Cells(next_row, 18).Value = six_month_low
                        ws.Cells(next_row, 19).Value = six_month_low_date
                        ws.Cells(next_row, 20).Value = three_month_high
                        ws.Cells(next_row, 21).Value = three_month_high_date
                        ws.Cells(next_row, 22).Value = three_month_low
                        ws.Cells(next_row, 23).Value = three_month_low_date
                        ws.Cells(next_row, 24).Value = month_high
                        ws.Cells(next_row, 25).Value = month_high_date
                        ws.Cells(next_row, 26).Value = month_low
                        ws.Cells(next_row, 27).Value = month_low_date
                        ws.Cells(next_row, 28).Value = week_high
                        ws.Cells(next_row, 29).Value = week_high_time
                        ws.Cells(next_row, 30).Value = week_low
                        ws.Cells(next_row, 31).Value = week_low_time
                        ws.Cells(next_row, 32).Value = day_high
                        ws.Cells(next_row, 33).Value = day_high_time
                        ws.Cells(next_row, 34).Value = day_low
                        ws.Cells(next_row, 35).Value = day_low_time
                        ws.Cells(next_row, 36).Value = hour_high
                        ws.Cells(next_row, 37).Value = hour_high_time
                        ws.Cells(next_row, 38).Value = hour_low
                        ws.Cells(next_row, 39).Value = hour_low_time
                        ws.Cells(next_row, 40).Value = thirty_min_high
                        ws.Cells(next_row, 41).Value = thirty_min_high_time
                        ws.Cells(next_row, 42).Value = thirty_min_low
                        ws.Cells(next_row, 43).Value = thirty_min_low_time
                        ws.Cells(next_row, 44).Value = fifteen_min_high
                        ws.Cells(next_row, 45).Value = fifteen_min_high_time
                        ws.Cells(next_row, 46).Value = fifteen_min_low
                        ws.Cells(next_row, 47).Value = fifteen_min_low_time
                        ws.Cells(next_row, 48).Value = avg_volume_1m  # Now an integer
                        ws.Cells(next_row, 49).Value = volume_ratio
                        ws.Cells(next_row, 50).Value = price_to_52w_high
                        ws.Cells(next_row, 51).Value = price_to_52w_low  # Now consistent with high
                        
                        # Update total processed count
                        total_processed += 1
                        
                        # Save every 5 instruments
                        if total_processed % 5 == 0:
                            wb.Save()
                            logger.info(f"Processed {total_processed} instruments so far")
                    else:
                        logger.error(f"Failed to fetch data for {symbol}")
                        
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {str(e)}")
                
                finally:
                    pbar.update(1)
                    # Small delay between instruments to avoid overwhelming the API
                    time.sleep(0.2)
        
        # Log final count
        logger.info(f"Successfully processed {total_processed} instruments out of {len(components_df)}")
        
        # Final save
        wb.Save()
        # Keep Excel open and visible
        excel.Visible = True
        excel.ScreenUpdating = True
        wb.Activate()
        logger.info("Completed fetching historical data. Excel file is open for viewing.")
            
    except Exception as e:
        logger.error(f"Error in fetch_nifty500_historical_data: {str(e)}")
    finally:
        if excel is not None:
            pythoncom.CoUninitialize()

def update_all_spot_data(ws, wb, instrument_dict, enctoken):
    """Update spot data for all symbols periodically."""
    try:
        # Initialize COM for this thread
        pythoncom.CoInitialize()
        
        last_row = ws.UsedRange.Rows.Count
        request_count = 0
        last_request_time = time.time()
        
        for row in range(3, last_row + 1):
            symbol = ws.Cells(row, 1).Value
            if symbol is None:
                continue
                
            tokens = instrument_dict.get(symbol)
            if tokens is None or tokens['spot'] is None:
                continue
            
            # Rate limiting
            current_time = time.time()
            if current_time - last_request_time >= 1:
                request_count = 0
                last_request_time = current_time
            
            if request_count >= 3:
                sleep_time = 1 - (current_time - last_request_time)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                request_count = 0
                last_request_time = time.time()
            
            # Fetch and update spot data
            spot_data = fetch_realtime_spot_data(tokens['spot']['token'], enctoken)
            if spot_data:
                update_spot_data(ws, row, spot_data)
                request_count += 1
            
            # Save periodically
            if row % 10 == 0:
                retry_excel_operation(lambda: wb.Save())
        
        return True
    except Exception as e:
        logger.error(f"Error updating all spot data: {str(e)}")
        return False
    finally:
        pythoncom.CoUninitialize()

def main():
    """Main function to run the script."""
    logger.info("Starting Nifty 500 historical data fetch")
    fetch_nifty500_historical_data(lookback_days=365, interval='day')

if __name__ == "__main__":
    main() 