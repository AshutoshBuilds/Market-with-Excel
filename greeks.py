"""Options Greeks calculator module."""
import math
from scipy.stats import norm
import numpy as np

class GreeksCalculator:
    def __init__(self):
        """Initialize Greeks calculator."""
        self.rate = 0.10  # Risk-free rate (10% as a starting point for Indian market)
        
    def _d1(self, S, K, T, r, sigma):
        """Calculate d1 parameter for Black-Scholes."""
        try:
            return (math.log(S/K) + (r + sigma**2/2)*T) / (sigma*math.sqrt(T))
        except (ValueError, ZeroDivisionError):
            return 0
            
    def _d2(self, d1, sigma, T):
        """Calculate d2 parameter for Black-Scholes."""
        try:
            return d1 - sigma*math.sqrt(T)
        except (ValueError, ZeroDivisionError):
            return 0

    def calculate_greeks(self, S, K, T, sigma, is_call=True):
        """
        Calculate all Greeks for an option.
        
        Parameters:
        - S: Current stock price
        - K: Strike price
        - T: Time to expiration (in years)
        - sigma: Volatility
        - is_call: True for call option, False for put option
        
        Returns:
        Dictionary containing all Greeks and IV
        """
        try:
            r = self.rate
            
            # Calculate d1 and d2
            d1 = self._d1(S, K, T, r, sigma)
            d2 = self._d2(d1, sigma, T)
            
            # Calculate N(d1) and N(d2)
            Nd1 = norm.cdf(d1 if is_call else -d1)
            Nd2 = norm.cdf(d2 if is_call else -d2)
            
            # Delta
            delta = Nd1 if is_call else Nd1 - 1
            
            # Gamma (same for calls and puts)
            gamma = norm.pdf(d1) / (S * sigma * math.sqrt(T))
            
            # Theta
            theta_factor = -(S * sigma * norm.pdf(d1)) / (2 * math.sqrt(T))
            r_factor = r * K * math.exp(-r * T) * Nd2 if is_call else -r * K * math.exp(-r * T) * (1 - Nd2)
            theta = theta_factor - r_factor
            
            # Vega (same for calls and puts)
            vega = S * math.sqrt(T) * norm.pdf(d1) * 0.01  # Scaled by 0.01 for percentage move
            
            return {
                'delta': round(delta, 4),
                'gamma': round(gamma, 6),
                'theta': round(theta, 4),
                'vega': round(vega, 4),
                'iv': round(sigma * 100, 2)  # Convert to percentage
            }
            
        except Exception as e:
            return {
                'delta': 0,
                'gamma': 0,
                'theta': 0,
                'vega': 0,
                'iv': 0
            }
            
    def estimate_iv(self, S, K, T, market_price, is_call=True, precision=0.0001):
        """
        Estimate implied volatility using Newton-Raphson method.
        """
        try:
            sigma = 0.5  # Initial guess
            max_iterations = 100
            
            for _ in range(max_iterations):
                greeks = self.calculate_greeks(S, K, T, sigma, is_call)
                d1 = self._d1(S, K, T, self.rate, sigma)
                d2 = self._d2(d1, sigma, T)
                
                if is_call:
                    price = S * norm.cdf(d1) - K * math.exp(-self.rate * T) * norm.cdf(d2)
                else:
                    price = K * math.exp(-self.rate * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
                
                diff = market_price - price
                
                if abs(diff) < precision:
                    return sigma * 100  # Convert to percentage
                    
                vega = S * math.sqrt(T) * norm.pdf(d1)
                sigma = sigma + diff/vega
                
                if sigma <= 0:
                    sigma = 0.0001
                    
            return sigma * 100
            
        except Exception:
            return 0 