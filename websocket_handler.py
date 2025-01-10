"""WebSocket handler for Excel updates."""
import json
import logging
import threading
import time
from typing import Dict, Any, Optional
from kiteconnect import KiteTicker
from datetime import datetime
import pytz
from src.utils.auth import ensure_valid_tokens
from src.utils.constants import SPOT_TOKENS, STRIKE_GAPS, SYMBOL_TOKEN_MAP
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
            print("\n" + "="*50)
            print(f"Received {len(ticks)} ticks at {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}")
            
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
                                'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
                            }
                            print(f"\nUpdated {symbol}:")
                            print(f"LTP: ₹{last_price:,.2f}")
                            print(f"Change: {change_percent:+.2f}%")
                            print(f"OHLC: {ohlc.get('open', 0.0)}/{ohlc.get('high', 0.0)}/{ohlc.get('low', 0.0)}/{ohlc.get('close', 0.0)}")
                            
                            # Subscribe to options if not already done
                            if not hasattr(self, 'options_subscribed'):
                                if len(self.market_data) >= len(self.index_tokens):
                                    print("\nAttempting to subscribe to options...")
                                    self._subscribe_options(self.market_data)
                                    self.options_subscribed = True
                        else:
                            # This is an options contract
                            # Extract strike and option type from symbol
                            if '_PE' in symbol or '_CE' in symbol:
                                strike = float(symbol.split('_')[0])
                                option_type = symbol.split('_')[1]  # PE or CE
                                
                                self.options_data[symbol] = {
                                    'token': token,
                                    'symbol': symbol,
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
                                    'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
                                }
                                print(f"\nUpdated {symbol}:")
                                print(f"Strike: {strike}")
                                print(f"LTP: ₹{last_price:,.2f}")
                                print(f"Change: {change_percent:+.2f}%")
                                print(f"OI: {tick.get('oi', 0):,}")
                else:
                    print(f"Unknown token: {token}")
            
            # Update Excel after processing all ticks
            try:
                market_data = self.get_market_data()
                options_data = self.get_options_data()
                print("\nUpdating Excel with:")
                print(f"Market Data ({len(market_data)} symbols):")
                for sym, data in market_data.items():
                    print(f"  {sym}: ₹{data.get('last_price', 0):,.2f} ({data.get('change_percent', 0):+.2f}%)")
                print(f"\nOptions Data ({len(options_data)} contracts)")
                for sym, data in options_data.items():
                    print(f"  {sym}: ₹{data.get('last_price', 0):,.2f} ({data.get('change_percent', 0):+.2f}%)")
                self.excel_updater.update_data(market_data, options_data)
                print(f"Excel updated with {len(market_data)} market symbols and {len(options_data)} options")
            except Exception as e:
                print(f"Error updating Excel: {str(e)}")
                logger.error(f"Error updating Excel: {str(e)}", exc_info=True)
                    
            print("="*50)
                    
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
            
            # Start heartbeat thread to ensure connection is alive
            def heartbeat():
                while self.connected:
                    try:
                        ist_now = datetime.now(pytz.timezone('Asia/Kolkata'))
                        print(f"\nHeartbeat - {ist_now.strftime('%H:%M:%S')} IST")
                        print(f"Connection Status: {'Connected' if self.connected else 'Disconnected'}")
                        print(f"Market Data Points: {len(self.market_data)}")
                        print(f"Options Data Points: {len(self.options_data)}")
                        
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
            
    def _subscribe_options(self, spot_prices: Dict[str, float]):
        """Subscribe to options based on spot prices."""
        try:
            options_tokens = []
            
            # Map index symbols to their spot symbols
            index_map = {
                'NIFTY 50': 'NIFTY',
                'NIFTY BANK': 'BANKNIFTY',
                'NIFTY FIN SERVICE': 'FINNIFTY',
                'NIFTY MID SELECT': 'MIDCPNIFTY',
                'SENSEX': 'SENSEX'
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
                
                # Create instrument lookup
                instrument_lookup = {}
                for row in instruments:
                    try:
                        instrument_token = int(row[0])
                        tradingsymbol = row[2]
                        name = row[3].strip('"')  # Remove quotes from name
                        expiry = row[5]
                        strike = float(row[6]) if row[6] else 0
                        instrument_type = row[9]
                        exchange = row[11]
                        
                        # Only process NFO instruments that are options
                        if exchange == 'NFO' and instrument_type in ['CE', 'PE']:
                            instrument_lookup[tradingsymbol] = {
                                'token': instrument_token,
                                'name': name,
                                'expiry': expiry,
                                'strike': strike,
                                'type': instrument_type
                            }
                    except (IndexError, ValueError) as e:
                        continue
                
                print(f"\nProcessing {len(instrument_lookup)} NFO instruments...")
                
                # Print a few example instruments to debug
                print("\nExample instruments:")
                for symbol, info in list(instrument_lookup.items())[:3]:
                    print(f"{symbol}: {info}")
                
                for spot_symbol, index_name in index_map.items():
                    # Get current spot price from the correct symbol
                    spot_data = next((data for sym, data in self.market_data.items() if sym == spot_symbol), None)
                    if spot_data:
                        spot_price = spot_data.get('last_price', 0)
                        if spot_price > 0:
                            print(f"\nProcessing {index_name} at spot price {spot_price}")
                            
                            # Calculate ATM strike
                            strike_gap = STRIKE_GAPS.get(spot_symbol, 50)
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
                                # Find nearest expiry
                                expiries = set(info['expiry'] for info in matching_instruments.values() if info['expiry'])
                                if expiries:
                                    nearest_expiry = min(expiries)
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
                                                self.token_symbol_map[str(token)] = f"{strike}_{opt_type}"
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