#!/usr/bin/env python3
# Find Short PUTs with Min Bid via TWS - Uses Black-Scholes pre-filtering
# Usage: python3 find_short_puts.py [SYMBOL] [--bid-min 0.80] [--dte-max 28] [--delta-min -0.50] [--delta-max -0.10] [--chains 3]

import sys
import logging
import math
from datetime import datetime
from scipy.stats import norm
from ib_insync import IB, Stock, Option

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s : %(message)s')
logger = logging.getLogger(__name__)

SYMBOL_DEFAULTS = {
    'ALV':   {'exchange': 'XETRA', 'currency': 'EUR'},
    'ASML':  {'exchange': 'AEB',   'currency': 'EUR'},
    'SAP':   {'exchange': 'XETRA', 'currency': 'EUR'},
    'CROX':  {'exchange': 'SMART', 'currency': 'USD'},
    'TREX':  {'exchange': 'SMART', 'currency': 'USD'},
    'AAPL':  {'exchange': 'SMART', 'currency': 'USD'},
    'MSFT':  {'exchange': 'SMART', 'currency': 'USD'},
    'ZIM':   {'exchange': 'SMART', 'currency': 'USD'},
    'CRWD':  {'exchange': 'SMART', 'currency': 'USD'},
    'IBM':   {'exchange': 'SMART', 'currency': 'USD'},
}

def connect_ib(client_id=100):
    ib = IB()
    ib.connect('127.0.0.1', 7496, clientId=client_id, timeout=10, readonly=True)
    return ib

def get_stock_and_chain(ib, symbol, month_idx, exchange, currency):
    defaults = SYMBOL_DEFAULTS.get(symbol.upper(), {'exchange': 'SMART', 'currency': 'USD'})
    ex = exchange if exchange != 'SMART' else defaults['exchange']
    cur = currency if currency != 'USD' else defaults['currency']
    
    stock = Stock(symbol, ex, cur)
    ib.qualifyContracts(stock)
    
    chains = ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
    chain = [c for c in chains if c.exchange == ex]
    if not chain:
        chain = [c for c in chains if c.expirations]
    if not chain:
        return None, None, None
    chain = chain[0]
    
    expirations = sorted(chain.expirations)
    if month_idx >= len(expirations):
        return None, None, None
    expiration = expirations[month_idx]
    return stock, chain, expiration

def bs_put_delta(S, K, T, r, sigma):
    """Black-Scholes Put Delta: N(d1) - 1"""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return -1.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    return norm.cdf(d1) - 1

def get_strike_range_for_delta(chain, stock_price, expiration, min_delta=-0.50, max_delta=-0.10, sigma=0.80):
    """Estimate strike range for target delta using Black-Scholes"""
    T = (datetime.strptime(expiration, '%Y%m%d') - datetime.now()).days / 365.0
    if T <= 0 or stock_price <= 0:
        return None, None
    
    r = 0.05
    min_strike = max_strike = None
    
    for strike in chain.strikes:
        delta = bs_put_delta(stock_price, strike, T, r, sigma)
        if max_delta <= delta <= min_delta:  # Put delta is negative
            if min_strike is None or strike < min_strike:
                min_strike = strike
            if max_strike is None or strike > max_strike:
                max_strike = strike
    
    return min_strike, max_strike

def scan_puts(symbol, bid_min=0.80, dte_max=28, delta_min=-0.50, delta_max=-0.10, chains=3, month_indices=None, exchange='SMART', currency='USD'):
    ib = connect_ib()
    try:
        results = []
        if month_indices is None:
            month_indices = list(range(chains))
        
        # Get first chain to know available expirations
        stock, chain, _ = get_stock_and_chain(ib, symbol, month_indices[0], exchange, currency)
        if not stock:
            return []
        
        print(f"\n{'='*80}")
        print(f"SCAN: {symbol} | Bid ≥ ${bid_min:.2f} | DTE ≤ {dte_max} | Delta {delta_max:.2f} to {delta_min:.2f}")
        print(f"{'='*80}")
        
        # Get stock price with polling
        stock_price = None
        try:
            ticker = ib.reqMktData(stock, '', False, False)
            start_time = datetime.now()
            while (datetime.now() - start_time).total_seconds() < 5:
                try:
                    ib.sleep(0.5)
                except Exception:
                    pass
                if ticker.last is not None or ticker.close is not None or (ticker.bid and ticker.ask):
                    break
            stock_price = ticker.last or ticker.close
            ib.cancelMktData(stock)
        except Exception as e:
            logger.error(f"get_stock_price({stock.symbol}): {e}")
        
        if not stock_price:
            print(f"⚠️  Kein Stock-Preis für {symbol} - Delta-Filter deaktiviert")
        
        for month_idx in month_indices:
            _, chain, expiration = get_stock_and_chain(ib, symbol, month_idx, exchange, currency)
            if not chain:
                continue
            
            exp_date = datetime.strptime(expiration, '%Y%m%d')
            dte = (exp_date - datetime.now()).days
            if dte > dte_max:
                continue
            
            # Black-Scholes pre-filter: only strikes in delta range
            if stock_price:
                min_strike, max_strike = get_strike_range_for_delta(chain, stock_price, expiration, delta_min, delta_max)
                if min_strike is None:
                    print(f"  {expiration}: Keine Strikes im Delta-Bereich")
                    continue
                print(f"  {expiration} (DTE={dte}d): Strike Range {min_strike}-{max_strike} (Stock: ${stock_price:.2f})")
            else:
                min_strike, max_strike = min(chain.strikes), max(chain.strikes)
            
            # Build options only for filtered strikes
            options = []
            for strike in chain.strikes:
                if strike < min_strike or strike > max_strike:
                    continue
                opt = Option(symbol, expiration, strike, 'P', exchange, tradingClass=symbol, multiplier=100)
                options.append(opt)
            
            if not options:
                continue
            
            ib.qualifyContracts(*options)
            
            # Request market data for filtered options only
            for opt in options:
                ib.reqMktData(opt, '', False, False)
            ib.sleep(2)
            
            for opt in options:
                ticker = ib.ticker(opt)
                bid = ticker.bid
                ask = ticker.ask
                last = ticker.last
                volume = ticker.volume
                
                if bid and bid >= bid_min:
                    delta = None
                    if hasattr(ticker, 'modelGreeks') and ticker.modelGreeks:
                        delta = ticker.modelGreeks.delta
                    
                    results.append({
                        'symbol': symbol,
                        'expiration': expiration,
                        'strike': opt.strike,
                        'dte': dte,
                        'bid': bid,
                        'ask': ask,
                        'last': last,
                        'volume': volume,
                        'delta': delta,
                        'localSymbol': opt.localSymbol,
                    })
            
            for opt in options:
                ib.cancelMktData(opt)
        
        # Filter by exact delta if Greeks available
        if results and any(r['delta'] is not None for r in results):
            results = [r for r in results if r['delta'] is None or (delta_max <= r['delta'] <= delta_min)]
        
        results.sort(key=lambda x: x['bid'], reverse=True)
        return results
        
    finally:
        ib.disconnect()

def print_results(results):
    if not results:
        print("Keine Optionen gefunden.")
        return
    
    print(f"\n{'Strike':>8} | {'Exp':>8} | {'DTE':>3} | {'Delta':>7} | {'Bid':>7} | {'Ask':>7} | {'Last':>7} | {'Vol':>6} | Symbol")
    print("-" * 90)
    for r in results:
        delta_str = f"{r['delta']:.4f}" if r['delta'] else "   n/a"
        print(f"{r['strike']:>8.1f} | {r['expiration'][4:]:>8} | {r['dte']:>3}d | {delta_str:>7} | ${r['bid']:>6.2f} | ${r['ask']:>6.2f} | ${r['last'] if r['last'] else 0:>6.2f} | {r['volume'] if r['volume'] else 0:>6} | {r['localSymbol']}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 find_short_puts.py SYMBOL [--bid-min 0.80] [--dte-max 28] [--dmin -0.50] [--dmax -0.10] [--chains 3]")
        print("\nExamples:")
        print("  python3 find_short_puts.py CROX")
        print("  python3 find_short_puts.py CROX --bid-min 0.80 --dte-max 28 --dmin -0.25 --dmax -0.15 --chains 3")
        print("  python3 find_short_puts.py ZIM --chains 5 --bid-min 0.20")
        sys.exit(1)
    
    symbol = sys.argv[1].upper()
    
    bid_min = 0.80
    dte_max = 28
    delta_min = -0.50
    delta_max = -0.10
    chains = 3
    month_indices = None
    exchange = 'SMART'
    currency = 'USD'
    
    args = sys.argv[2:]
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in ['--bid-min', '--bmin'] and i + 1 < len(args):
            bid_min = float(args[i + 1])
            i += 1
        elif arg in ['--dte-max', '--dte'] and i + 1 < len(args):
            dte_max = int(args[i + 1])
            i += 1
        elif arg in ['--delta-min', '--dmin'] and i + 1 < len(args):
            delta_min = float(args[i + 1])
            i += 1
        elif arg in ['--delta-max', '--dmax'] and i + 1 < len(args):
            delta_max = float(args[i + 1])
            i += 1
        elif arg in ['--chains', '--num-chains'] and i + 1 < len(args):
            chains = int(args[i + 1])
            i += 1
        elif arg.isdigit():
            if month_indices is None:
                month_indices = []
            month_indices.append(int(arg))
        elif arg in ['SMART', 'XETRA', 'AEB', 'NYSE', 'NASDAQ', 'FWB', 'IBIS']:
            exchange = arg
        elif arg in ['USD', 'EUR']:
            currency = arg
        i += 1
    
    results = scan_puts(symbol, bid_min, dte_max, delta_min, delta_max, chains, month_indices, exchange, currency)
    print_results(results)
    
    if results:
        print(f"\n✅ {len(results)} Optionen gefunden mit Bid ≥ ${bid_min:.2f}")
    else:
        print(f"\n❌ Keine Optionen mit Bid ≥ ${bid_min:.2f} gefunden")

if __name__ == '__main__':
    main()