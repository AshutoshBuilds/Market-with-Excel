"""WebSocket handler for Excel updates."""
import json
import logging
import threading
import time
from typing import Dict, Any, Optional
from kiteconnect import KiteTicker
from datetime import datetime, timedelta
import pytz
from src.utils.auth import ensure_valid_tokens
from src.utils.constants import SPOT_TOKENS, STRIKE_GAPS, SYMBOL_TOKEN_MAP, FUTURES_NAME_MAP
import requests

logger = logging.getLogger(__name__)
# Set websocket logger to WARNING to reduce noise
logging.getLogger('websocket').setLevel(logging.WARNING)

class ExcelWebSocketHandler:
    def __init__(self, excel_updater, token: str):
        """Initialize WebSocket handler."""
        self.excel_updater = excel_updater
        self.token = token
        self.connected = False
        self.market_data: Dict[str, Dict[str, Any]] = {}
        self.options_data: Dict[str, Dict[str, Any]] = {}
        self.futures_data: Dict[str, Dict[str, Any]] = {}  # Add futures data dictionary
        self._lock = threading.Lock()
        self._reconnect_count = 0
        self.MAX_RECONNECTS = 5
        self.RECONNECT_DELAY = 5  # seconds
        self.last_connect_attempt = 0
        self.backoff_time = 5  # Initial backoff time in seconds
        
        # Initialize token-symbol mappings
        self.spot_tokens = SPOT_TOKENS
        self.token_symbol_map = {str(k): v for k, v in SYMBOL_TOKEN_MAP.items()}  # Convert keys to strings
        self.index_tokens = {str(info['token']) for info in SPOT_TOKENS.values()}  # Convert to strings
        
        # Wait before first connection attempt
        time.sleep(2)
        self._connect()
        
    def _connect(self):
        """Connect to WebSocket with rate limiting."""
        try:
            current_time = time.time()
            time_since_last_attempt = current_time - self.last_connect_attempt
            
            # If we're reconnecting too quickly, wait
            if time_since_last_attempt < self.backoff_time:
                wait_time = self.backoff_time - time_since_last_attempt
                print(f"\nRate limit cooldown - waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
            
            self.last_connect_attempt = time.time()
            
            # Get fresh tokens
            print("\nGetting fresh tokens...")
            enctoken, access_token = ensure_valid_tokens()
            self.enctoken = enctoken  # Store enctoken as instance variable
            
            # Initialize WebSocket
            print("Initializing WebSocket...")
            self.kws = KiteTicker(api_key="kitefront", access_token=access_token)
            
            # Set up callbacks
            self.kws.on_ticks = self._on_ticks
            self.kws.on_connect = self._on_connect
            self.kws.on_close = self._on_close
            self.kws.on_error = self._on_error
            self.kws.on_reconnect = self._on_reconnect
            self.kws.on_noreconnect = self._on_noreconnect
            
            # Connect to WebSocket
            print("Connecting to WebSocket...")
            self.kws.connect(threaded=True)
            
            # Wait for connection
            timeout = 30  # Increased timeout
            start_time = time.time()
            while not self.connected and time.time() - start_time < timeout:
                time.sleep(0.1)
                
            if not self.connected:
                raise Exception("WebSocket connection timeout")
                
            print("WebSocket connected successfully!")
            self._reconnect_count = 0  # Reset reconnect counter on successful connection
            self.backoff_time = 5  # Reset backoff time on successful connection
            
        except Exception as e:
            print(f"Error connecting to WebSocket: {str(e)}")
            self._handle_reconnect()
            
    def _on_ticks(self, ws, ticks):
        """Handle incoming tick data."""
        try:
            current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
            
            # Process each tick
            for tick in ticks:
                token = tick['instrument_token']
                str_token = str(token)
                symbol = self.token_symbol_map.get(str_token, 'Unknown')
                
                # Process only if we know this token
                if str_token in self.token_symbol_map:
                    # Extract OHLC data
                    ohlc = tick.get('ohlc', {})
                    
                    # Extract depth data
                    depth = tick.get('depth', {})
                    buy_depth = depth.get('buy', [{}])[0] if depth.get('buy') else {}
                    sell_depth = depth.get('sell', [{}])[0] if depth.get('sell') else {}
                    
                    # Calculate change percent
                    last_price = tick.get('last_price', 0.0)
                    change = tick.get('change', 0.0)
                    change_percent = (change / (last_price - change)) * 100 if last_price != change else 0
                    
                    # Update market data with thread safety
                    with self._lock:
                        if str_token in self.index_tokens:
                            # This is a spot index
                            self.market_data[symbol] = {
                                'token': token,
                                'symbol': symbol,
                                'last_price': last_price,
                                'change': change,
                                'change_percent': change_percent,
                                'volume': tick.get('volume', 0),
                                'open': ohlc.get('open', 0.0),
                                'high': ohlc.get('high', 0.0),
                                'low': ohlc.get('low', 0.0),
                                'close': ohlc.get('close', 0.0),
                                'bid_price': buy_depth.get('price', 0.0),
                                'bid_qty': buy_depth.get('quantity', 0),
                                'ask_price': sell_depth.get('price', 0.0),
                                'ask_qty': sell_depth.get('quantity', 0),
                                'timestamp': current_time
                            }
                            
                        elif symbol.endswith('FUT'):
                            # This is a futures contract
                            # Calculate volume from trades
                            trades = tick.get('trades', [])
                            if trades:
                                volume = sum(trade.get('quantity', 0) for trade in trades)
                                # Add to cumulative volume
                                self.last_traded_volumes[symbol] = self.last_traded_volumes.get(symbol, 0) + volume
                            else:
                                # If no trades in this tick, use the volume field
                                tick_volume = tick.get('volume', 0)
                                if tick_volume > self.last_traded_volumes.get(symbol, 0):
                                    self.last_traded_volumes[symbol] = tick_volume
                            
                            self.futures_data[symbol] = {
                                'token': token,
                                'symbol': symbol,
                                'last_price': last_price,
                                'change': change,
                                'change_percent': change_percent,
                                'volume': self.last_traded_volumes.get(symbol, 0),
                                'open': ohlc.get('open', 0.0),
                                'high': ohlc.get('high', 0.0),
                                'low': ohlc.get('low', 0.0),
                                'close': ohlc.get('close', 0.0),
                                'oi': tick.get('oi', 0),
                                'bid_price': buy_depth.get('price', 0.0),
                                'bid_qty': buy_depth.get('quantity', 0),
                                'ask_price': sell_depth.get('price', 0.0),
                                'ask_qty': sell_depth.get('quantity', 0),
                                'timestamp': current_time
                            }
                            
                        elif 'CE' in symbol or 'PE' in symbol:
                            # This is an options contract
                            try:
                                print(f"\nProcessing options tick for symbol: {symbol}")
                                # Extract strike and option type from the tradingsymbol
                                # Example tradingsymbol format: NIFTY2516123400CE
                                # First, identify the index name
                                index_name = None
                                if symbol.startswith('NIFTY'):
                                    index_name = 'NIFTY'
                                elif symbol.startswith('BANKNIFTY'):
                                    index_name = 'BANKNIFTY'
                                elif symbol.startswith('FINNIFTY'):
                                    index_name = 'FINNIFTY'
                                elif symbol.startswith('MIDCPNIFTY'):
                                    index_name = 'MIDCPNIFTY'
                                elif symbol.startswith('SENSEX'):
                                    index_name = 'SENSEX'
                                
                                print(f"Identified index name: {index_name}")
                                
                                if index_name:
                                    # Extract strike and option type from the tradingsymbol
                                    # The strike is the numeric part before CE/PE
                                    strike_str = ''.join(filter(str.isdigit, symbol.split('CE')[0].split('PE')[0][-6:]))
                                    strike = float(strike_str)
                                    option_type = 'CE' if 'CE' in symbol else 'PE'
                                    
                                    print(f"Extracted strike: {strike}, option type: {option_type}")
                                    
                                    # Create the full symbol for options data storage
                                    full_symbol = f"{index_name}_{strike}_{option_type}"
                                    print(f"Created full symbol: {full_symbol}")
                                    
                                    # Store options data with full symbol
                                    self.options_data[full_symbol] = {
                                        'token': token,
                                        'symbol': full_symbol,
                                        'strike': strike,
                                        'option_type': option_type,
                                        'last_price': last_price,
                                        'change': change,
                                        'change_percent': change_percent,
                                        'volume': tick.get('volume', 0),
                                        'oi': tick.get('oi', 0),
                                        'bid_price': buy_depth.get('price', 0.0),
                                        'bid_qty': buy_depth.get('quantity', 0),
                                        'ask_price': sell_depth.get('price', 0.0),
                                        'ask_qty': sell_depth.get('quantity', 0),
                                        'timestamp': current_time
                                    }
                                    print(f"Updated options data for {full_symbol}:")
                                    print(f"  LTP: {last_price}")
                                    print(f"  OI: {tick.get('oi', 0)}")
                                    print(f"  Volume: {tick.get('volume', 0)}")
                                    print(f"  Bid: {buy_depth.get('price', 0.0)} x {buy_depth.get('quantity', 0)}")
                                    print(f"  Ask: {sell_depth.get('price', 0.0)} x {sell_depth.get('quantity', 0)}")
                                else:
                                    print(f"Could not determine index name from tradingsymbol: {symbol}")
                            except Exception as e:
                                print(f"Error processing options tick for {symbol}: {str(e)}")
                                print(f"Full tick data: {tick}")
                else:
                    print(f"Unknown token: {token}")
            
            # Update Excel after processing all ticks
            try:
                market_data = self.get_market_data()
                futures_data = self.get_futures_data()
                options_data = self.get_options_data()
                
                # Print some debug info about the data being sent to Excel
                print(f"\nSending to Excel - Options data count: {len(options_data)}")
                if options_data:
                    # Print a sample of the options data
                    sample_symbol = next(iter(options_data))
                    print(f"Sample options data - {sample_symbol}: {options_data[sample_symbol]}")
                
                self.excel_updater.update_data(market_data, futures_data, options_data)
            except Exception as e:
                print(f"Error updating Excel: {str(e)}")
                logger.error(f"Error updating Excel: {str(e)}", exc_info=True)
                    
        except Exception as e:
            print(f"Error processing ticks: {str(e)}")
            logger.error(f"Error processing ticks: {str(e)}", exc_info=True)
            
    def _on_connect(self, ws, response):
        """Handle WebSocket connection open."""
        print("\nWebSocket connected!")
        self.connected = True
        
        try:
            # Get spot tokens
            spot_tokens = [info['token'] for info in SPOT_TOKENS.values()]
            print(f"Subscribing to spot tokens: {spot_tokens}")
            
            # Subscribe to spot tokens
            self.kws.subscribe(spot_tokens)
            
            # Set mode to full for spot tokens
            self.kws.set_mode(self.kws.MODE_FULL, spot_tokens)
            
            print("Successfully subscribed to spot tokens")
            
            # Subscribe to futures
            self._subscribe_futures()
            
            # Subscribe to options after a short delay to ensure we have spot prices
            def delayed_options_subscription():
                time.sleep(2)  # Wait for 2 seconds to get spot data
                if self.market_data:
                    print("\nAttempting to subscribe to options...")
                    self._subscribe_options(self.market_data)
                    self.options_subscribed = True
            
            # Start options subscription thread
            options_thread = threading.Thread(target=delayed_options_subscription)
            options_thread.daemon = True
            options_thread.start()
            
            # Start heartbeat thread to ensure connection is alive
            def heartbeat():
                while self.connected:
                    try:
                        ist_now = datetime.now(pytz.timezone('Asia/Kolkata'))
                        print(f"\nHeartbeat - {ist_now.strftime('%H:%M:%S')} IST")
                        print(f"Connection Status: {'Connected' if self.connected else 'Disconnected'}")
                        print(f"Market Data Points: {len(self.market_data)}")
                        print(f"Options Data Points: {len(self.options_data)}")
                        print(f"Futures Data Points: {len(self.futures_data)}")
                        
                        time.sleep(5)  # Check every 5 seconds
                    except Exception as e:
                        logger.error(f"Error in heartbeat: {str(e)}")
                        time.sleep(1)
            
            # Start heartbeat thread
            self.heartbeat_thread = threading.Thread(target=heartbeat)
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()
            
        except Exception as e:
            logger.error(f"Error in subscription: {str(e)}")
            print(f"Error in subscription: {str(e)}")
            self._handle_reconnect()

    def _subscribe_futures(self):
        """Subscribe to futures instruments."""
        try:
            print("\nFetching futures instruments...")
            headers = {
                'Authorization': f'enctoken {self.enctoken}',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/csv',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin'
            }
            
            futures_tokens = []
            
            # Get current date for expiry comparison
            current_date = datetime.now(pytz.timezone('Asia/Kolkata')).date()
            
            # Map index names to their futures symbols and exchanges
            index_futures_map = {
                'NFO': {
                    'NIFTY 50': 'NIFTY',
                    'NIFTY BANK': 'BANKNIFTY',
                    'NIFTY FIN SERVICE': 'FINNIFTY',
                    'NIFTY MID SELECT': 'MIDCPNIFTY'
                },
                'BFO': {
                    'SENSEX': 'SENSEX'
                }
            }
            
            # Store last traded volumes for each symbol
            self.last_traded_volumes = {}
            
            # Fetch from NFO and BFO with their respective indices
            for exchange, symbols in index_futures_map.items():
                try:
                    print(f"\nFetching {exchange} instruments...")
                    response = requests.get('https://api.kite.trade/instruments', headers=headers)
                    print(f"{exchange} response status: {response.status_code}")
                    
                    if response.status_code == 200:
                        print(f"Successfully fetched {exchange} instruments")
                        instruments = response.text.strip().split('\n')
                        instruments = [row.split(',') for row in instruments[1:]]  # Skip header
                        
                        # Store nearest expiry contracts for each symbol
                        nearest_expiry_contracts = {}
                        
                        for row in instruments:
                            try:
                                if len(row) < 12:  # Basic validation
                                    continue
                                
                                instrument_token = int(row[0])
                                tradingsymbol = row[2]
                                name = row[3].strip('"')
                                expiry = datetime.strptime(row[5], '%Y-%m-%d').date() if row[5] else None
                                instrument_type = row[9]
                                segment = row[10]
                                exchange_from_row = row[11]
                                
                                # Only process instruments from the current exchange
                                if exchange_from_row != exchange:
                                    continue
                                
                                # Process only futures instruments
                                if instrument_type == 'FUT':
                                    # Check for each symbol we're interested in
                                    for display_name, futures_symbol in symbols.items():
                                        if tradingsymbol.startswith(futures_symbol):
                                            # Only consider if expiry is in the future
                                            if expiry and expiry >= current_date:
                                                # Initialize or update nearest expiry contract
                                                if display_name not in nearest_expiry_contracts or \
                                                   expiry < nearest_expiry_contracts[display_name]['expiry']:
                                                    nearest_expiry_contracts[display_name] = {
                                                        'token': instrument_token,
                                                        'symbol': tradingsymbol,
                                                        'expiry': expiry,
                                                        'display_name': display_name
                                                    }
                                                    print(f"Found {futures_symbol} futures expiring on {expiry}")
                            
                            except (IndexError, ValueError) as e:
                                print(f"Error processing row: {str(e)}")
                                continue
                        
                        # Subscribe to the nearest expiry contracts
                        for contract_info in nearest_expiry_contracts.values():
                            token = contract_info['token']
                            display_name = contract_info['display_name']
                            expiry = contract_info['expiry']
                            
                            futures_tokens.append(token)
                            # Store with the format expected by the Excel updater
                            display_name_map = {
                                'NIFTY 50': 'NIFTY',
                                'NIFTY BANK': 'BANKNIFTY',
                                'NIFTY FIN SERVICE': 'FINNIFTY',
                                'NIFTY MID SELECT': 'MIDCPNIFTY',
                                'SENSEX': 'SENSEX'
                            }
                            excel_symbol = f"{display_name_map[display_name]} FUT"
                            self.token_symbol_map[str(token)] = excel_symbol
                            # Initialize volume tracking for this symbol
                            self.last_traded_volumes[excel_symbol] = 0
                            print(f"Added nearest futures token {token} for {display_name} expiring on {expiry}")
                    
                    else:
                        print(f"Error getting {exchange} instruments: {response.status_code}")
                        print(f"Response text: {response.text}")
                        
                except Exception as e:
                    print(f"Error fetching {exchange} instruments: {str(e)}")
                    print(f"Full error: ", exc_info=True)
            
            if futures_tokens:
                print(f"\nSubscribing to {len(futures_tokens)} futures tokens: {futures_tokens}")
                # Subscribe with LTP mode first for faster updates
                self.kws.subscribe(futures_tokens)
                self.kws.set_mode(self.kws.MODE_LTP, futures_tokens)
                print("Subscribed to futures tokens in LTP mode")
                
                # Then set FULL mode for detailed data
                time.sleep(0.5)  # Small delay before changing mode
                self.kws.set_mode(self.kws.MODE_FULL, futures_tokens)
                print("Changed futures tokens to FULL mode")
                
                print("Token to symbol mapping for futures:")
                for token in futures_tokens:
                    print(f"{token}: {self.token_symbol_map.get(str(token), 'Unknown')}")
            else:
                print("No valid futures tokens found!")
                
        except Exception as e:
            logger.error(f"Error subscribing to futures: {str(e)}")
            print(f"Error subscribing to futures: {str(e)}")
            print("Full error: ", exc_info=True)
            
    def _subscribe_options(self, market_data):
        """Subscribe to options based on spot prices."""
        try:
            print("\nSubscribing to options...")
            options_tokens = []
            
            # Map index symbols to their spot symbols and expiry types
            index_map = {
                'NIFTY 50': {'symbol': 'NIFTY', 'expiry_type': 'weekly'},
                'NIFTY BANK': {'symbol': 'BANKNIFTY', 'expiry_type': 'monthly'},
                'NIFTY FIN SERVICE': {'symbol': 'FINNIFTY', 'expiry_type': 'monthly'},
                'NIFTY MID SELECT': {'symbol': 'MIDCPNIFTY', 'expiry_type': 'monthly'},
                'SENSEX': {'symbol': 'SENSEX', 'expiry_type': 'weekly'}
            }
            
            print("\nFetching NFO instruments...")
            # Get instruments from Kite API
            headers = {
                'Authorization': f'enctoken {self.enctoken}',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/csv',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin'
            }
            
            # Get NFO instruments
            response = requests.get('https://api.kite.trade/instruments', headers=headers)
            if response.status_code == 200:
                print("Successfully fetched NFO instruments")
                instruments = response.text.strip().split('\n')
                instruments = [row.split(',') for row in instruments[1:]]  # Skip header
                
                # Get current date for expiry comparison
                current_date = datetime.now(pytz.timezone('Asia/Kolkata')).date()
                
                # Create instrument lookup
                instrument_lookup = {}
                for row in instruments:
                    try:
                        instrument_token = int(row[0])
                        tradingsymbol = row[2]
                        name = row[3].strip('"')  # Remove quotes from name
                        expiry = datetime.strptime(row[5], '%Y-%m-%d').date() if row[5] else None
                        strike = float(row[6]) if row[6] else 0
                        instrument_type = row[9]  # CE or PE
                        exchange = row[11]
                        
                        # Only process NFO instruments that are options and have future expiry
                        if exchange == 'NFO' and instrument_type in ['CE', 'PE'] and expiry and expiry >= current_date:
                            # Store with all details needed for filtering
                            instrument_lookup[tradingsymbol] = {
                                'token': instrument_token,
                                'name': name,
                                'expiry': expiry,
                                'strike': strike,
                                'type': instrument_type,
                                'symbol': tradingsymbol
                            }
                    except (IndexError, ValueError) as e:
                        continue
                
                print(f"\nProcessing {len(instrument_lookup)} NFO instruments...")
                
                for spot_symbol, index_info in index_map.items():
                    index_name = index_info['symbol']
                    expiry_type = index_info['expiry_type']
                    
                    # Get current spot price from the correct symbol
                    spot_data = next((data for sym, data in market_data.items() if sym == spot_symbol), None)
                    if spot_data:
                        spot_price = spot_data.get('last_price', 0)
                        if spot_price > 0:
                            print(f"\nProcessing {index_name} at spot price {spot_price}")
                            
                            # Calculate ATM strike
                            strike_gap = {
                                'NIFTY 50': 50,
                                'NIFTY BANK': 100,
                                'NIFTY FIN SERVICE': 50,
                                'NIFTY MID SELECT': 50,
                                'SENSEX': 100
                            }.get(spot_symbol, 50)
                            
                            atm_strike = round(spot_price / strike_gap) * strike_gap
                            print(f"ATM strike: {atm_strike}")
                            
                            # Get 5 strikes above and below ATM
                            strikes = [atm_strike + (i - 5) * strike_gap for i in range(11)]
                            
                            # Find instruments for this index
                            matching_instruments = {
                                symbol: info for symbol, info in instrument_lookup.items()
                                if symbol.startswith(index_name)  # Match by index name prefix
                            }
                            
                            if matching_instruments:
                                # Get all expiries for this index
                                expiries = sorted(set(info['expiry'] for info in matching_instruments.values() if info['expiry']))
                                
                                if expiries:
                                    # For weekly expiry (NIFTY and SENSEX), get the nearest expiry
                                    # For monthly expiry (others), get the nearest monthly expiry
                                    if expiry_type == 'weekly':
                                        nearest_expiry = min(expiries)
                                    else:
                                        # Filter for monthly expiry (last Thursday of the month)
                                        monthly_expiries = [
                                            expiry for expiry in expiries
                                            if expiry.weekday() == 3  # Thursday
                                            and (expiry + timedelta(days=7)).month != expiry.month  # Last Thursday
                                        ]
                                        if monthly_expiries:
                                            nearest_expiry = min(monthly_expiries)
                                        else:
                                            nearest_expiry = min(expiries)  # Fallback to nearest expiry
                                    
                                    print(f"Found {len(matching_instruments)} matching instruments")
                                    print(f"Using expiry: {nearest_expiry}")
                                    
                                    # Add PE and CE tokens for each strike
                                    for strike in strikes:
                                        # Try to find PE and CE contracts
                                        for symbol, info in matching_instruments.items():
                                            if (info['expiry'] == nearest_expiry and 
                                                abs(info['strike'] - strike) < 0.01):  # Compare with small tolerance
                                                
                                                token = info['token']
                                                opt_type = info['type']
                                                
                                                options_tokens.append(token)
                                                # Store the full tradingsymbol in the token map
                                                self.token_symbol_map[str(token)] = info['symbol']
                                                print(f"Added {opt_type} token {token} for {index_name} {strike}")
                
                if options_tokens:
                    print(f"\nSubscribing to {len(options_tokens)} options tokens")
                    self.kws.subscribe(options_tokens)
                    self.kws.set_mode(self.kws.MODE_FULL, options_tokens)
                    print("Successfully subscribed to options tokens")
                else:
                    print("\nNo valid options tokens found to subscribe!")
            else:
                print(f"Error getting instruments: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error subscribing to options: {str(e)}", exc_info=True)
            print(f"Error subscribing to options: {str(e)}")
            print("Full error: ", exc_info=True)
            
    def _on_close(self, ws, code, reason):
        """Handle WebSocket connection close."""
        logger.info(f"WebSocket connection closed: {code} - {reason}")
        self.connected = False
        self._handle_reconnect()
        
    def _on_error(self, ws, code, reason):
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error: {code} - {reason}")
        self.connected = False
        self._handle_reconnect()
        
    def _on_reconnect(self, ws, attempts_count):
        """Handle WebSocket reconnect."""
        logger.info(f"WebSocket reconnecting... {attempts_count} attempts")
        
    def _on_noreconnect(self, ws):
        """Handle WebSocket no reconnect."""
        logger.error("WebSocket failed to reconnect")
        
    def _handle_reconnect(self):
        """Handle reconnection logic with exponential backoff."""
        if self._reconnect_count < self.MAX_RECONNECTS:
            self._reconnect_count += 1
            # Exponential backoff: 5s, 10s, 20s, 40s, 80s
            self.backoff_time = min(300, self.backoff_time * 2)  # Cap at 5 minutes
            print(f"\nAttempting reconnection {self._reconnect_count}/{self.MAX_RECONNECTS}")
            print(f"Using exponential backoff - waiting {self.backoff_time} seconds...")
            time.sleep(self.backoff_time)
            self._connect()
        else:
            print("\nMax reconnection attempts reached. Please:")
            print("1. Check your internet connection")
            print("2. Ensure your authentication tokens are valid")
            print("3. Wait a few minutes before trying again")
            print("4. Restart the application")
            logger.error("Max reconnection attempts reached")
            
    def close(self):
        """Close WebSocket connection."""
        self.connected = False  # This will stop the update thread
        if hasattr(self, 'kws'):
            self.kws.close()
            logger.info("WebSocket connection closed")
            
        # Wait for update thread to finish
        if hasattr(self, 'update_thread') and self.update_thread.is_alive():
            self.update_thread.join(timeout=5)
            logger.info("Excel update thread stopped")
            
    def get_market_data(self) -> Dict[str, Dict[str, Any]]:
        """Get a copy of the current market data."""
        with self._lock:
            return self.market_data.copy()
            
    def get_options_data(self) -> Dict[str, Dict[str, Any]]:
        """Get a copy of the current options data."""
        with self._lock:
            return self.options_data.copy()
            
    def get_futures_data(self) -> Dict[str, Dict[str, Any]]:
        """Get a copy of the current futures data."""
        with self._lock:
            return self.futures_data.copy()
            
    def _update_options_chain(self, symbol: str, spot_price: float):
        """Update options chain data for the given symbol."""
        try:
            # Calculate ATM strike
            strike_gap = STRIKE_GAPS.get(symbol, 50)  # Get strike gap from constants or use default
            atm_strike = round(spot_price / strike_gap) * strike_gap
            
            # Get strikes around ATM
            num_strikes = 10  # 10 strikes above and below ATM
            strikes = [atm_strike + (i - num_strikes) * strike_gap for i in range(2 * num_strikes + 1)]
            
            # Update options data with thread safety
            with self._lock:
                for strike in strikes:
                    # Process PE options
                    pe_symbol = f"{strike}_PE"
                    pe_data = self.options_data.get(pe_symbol, {})
                    if pe_data:
                        pe_data['strike'] = strike
                        pe_data['option_type'] = 'PE'
                        pe_data['is_atm'] = strike == atm_strike
                    
                    # Process CE options
                    ce_symbol = f"{strike}_CE"
                    ce_data = self.options_data.get(ce_symbol, {})
                    if ce_data:
                        ce_data['strike'] = strike
                        ce_data['option_type'] = 'CE'
                        ce_data['is_atm'] = strike == atm_strike
                
            logger.debug(f"Updated options chain for {symbol} with {len(strikes)} strikes")
            
        except Exception as e:
            logger.error(f"Error updating options chain for {symbol}: {str(e)}") 
                
            logger.debug(f"Updated options chain for {symbol} with {len(strikes)} strikes")
            
        except Exception as e:
            logger.error(f"Error updating options chain for {symbol}: {str(e)}") 

