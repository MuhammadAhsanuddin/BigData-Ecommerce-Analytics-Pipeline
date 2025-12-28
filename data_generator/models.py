"""
Statistical Models for E-Commerce Data Generation
=================================================

This module defines the statistical distributions used to generate
realistic e-commerce transaction data.

Models Used:
1. Poisson Distribution - Order arrival rate (lambda=50/min)
2. Beta Distribution - Peak hour traffic patterns
3. Zipf Distribution - Customer purchase frequency (power law)
4. Zipf Distribution - Product popularity (long tail)
5. Geometric Distribution - Quantity per order
6. Log-Normal Distribution - Product pricing
7. Exponential Distribution - Shipping costs
8. Normal Distribution - Tax rates
9. Gamma Distribution - Payment processing time
"""

import numpy as np

class EcommerceModels:
    """Statistical models for realistic e-commerce data generation"""
    
    @staticmethod
    def order_arrival_rate(base_rate=50):
        """Poisson distribution for order arrivals per minute"""
        return np.random.poisson(base_rate)
    
    @staticmethod
    def peak_hour_multiplier(is_peak=True):
        """Beta distribution for traffic patterns"""
        if is_peak:
            return np.random.beta(2, 5) + 1.5  # Higher during peak
        return np.random.beta(5, 2) * 0.5  # Lower off-peak
    
    @staticmethod
    def customer_selection(pool_size, alpha=1.5):
        """Zipf distribution - 20% customers make 80% orders"""
        return int(np.random.zipf(alpha) - 1) % pool_size
    
    @staticmethod
    def product_popularity(pool_size, alpha=1.3):
        """Zipf distribution - long tail product sales"""
        return int(np.random.zipf(alpha) - 1) % pool_size
    
    @staticmethod
    def order_quantity(p=0.6):
        """Geometric distribution - most orders have 1-2 items"""
        return np.random.geometric(p)
    
    @staticmethod
    def product_price(mean=3.5, sigma=1.0):
        """Log-normal distribution - realistic price distribution"""
        return np.random.lognormal(mean, sigma)
    
    @staticmethod
    def shipping_cost(scale=5.0, base=2.99):
        """Exponential distribution for variable shipping"""
        return np.random.exponential(scale) + base
    
    @staticmethod
    def tax_rate(mean=0.08, std=0.01):
        """Normal distribution around 8% tax"""
        return np.random.normal(mean, std)
    
    @staticmethod
    def processing_time(shape=2, scale=0.5):
        """Gamma distribution for payment processing"""
        return np.random.gamma(shape, scale)