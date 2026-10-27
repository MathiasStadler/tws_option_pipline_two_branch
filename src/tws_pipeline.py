#!/usr/bin/env python3
# Pure TWS Pipeline: optimierte Version mit Delta-Pre-Filtering
# Usage: python3 tws_pipeline.py <SYMBOL> [MONTH_INDEX] [EXCHANGE] [CURRENCY]

import sys
import csv
import logging
import math
from datetime import datetime
from ib_insync import IB, Stock, Option

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s : %(message)s')
logger = logging.getLogger(__name__)

HEADERS = [
    "conid", "symbol", "right", "expiration", "strike", "multiplier",
    "bid", "ask", "last", "close", "volume",
    "delta", "gamma", "theta", "vega", "impliedVol",
    "bidSize", "askSize", "high", "low", "openPrice", "openInterest",
    "put_profit"
]

SYMBOL_DEFAULTS = {
    'ALV':   {'exchange': 'SMART', 'currency': 'EUR'},
    'ASML':  {'exchange': 'SMART', 'currency': 'EUR'},
    'SAP':   {'exchange': 'SMART', 'currency': 'EUR'},
    'SIE':   {'exchange': 'SMART', 'currency': 'EUR'},
    'VOW3':  {'exchange': 'SMART', 'currency': 'EUR'},
    'BMW':   {'exchange': 'SMART', 'currency': 'EUR'},
    'DTE':   {'exchange': 'SMART', 'currency': 'EUR'},
    'BAYN':  {'exchange': 'SMART', 'currency': 'EUR'},
    'BAS':   {'exchange': 'SMART', 'currency': 'EUR'},
    'DHL':   {'exchange': 'SMART', 'currency': 'EUR'},
    'CROX':  {'exchange': 'SMART', 'currency': 'USD'},
    'TREX':  {'exchange': 'SMART', 'currency': 'USD'},
    'AAPL':  {'exchange': 'SMART', 'currency': 'USD'},
    'MSFT':  {'exchange': 'SMART', 'currency': 'USD'},
}

# TWS API Rate Limits (official IB documentation)
TWS_RATE_LIMIT_RPS = 50
REQ_MKT_DATA_SLEEP = 1.0 / 50 * 1.5  # 0.03s with safety margin

# Black-Scholes Delta Approximation für Put-Optionen
def bs_put_delta(S, K, T, r, sigma):
    """
    Black-Scholes Put Delta Approximation
    S = Spot (Underlying Price)
    K = Strike
    T = Time to Expiry (in Jahren)
    r = Risk-free rate (approx 0.05 für USD)
    sigma = Implied Volatility (geschätzt)
    Returns: Put Delta (negativ)
    """
    if sigma <= 0 or T <= 0:
        return None
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    # Put Delta = N(d1) - 1 = -N(-d1)
    delta = 0.5 * (1 + math.erf(d1 / math.sqrt(2))) - 1
    return delta

def estimate_atm_iv(chain, stock_price):
    """
    Schätzt ATM IV basierend auf verfügbaren Strikes nahe am Geld
    Nutzt die Strike-Liste als Proxy (breitere Streuung = höhere IV)
    """
    if not chain.strikes:
        return 0.3  # Default fallback
    
    # Finde ATM Strike
    atm_strike = min(chain.strikes, key=lambda x: abs(x - stock_price))
    
    # Breite der Strike-Liste als IV-Proxy
    strike_range = max(chain.strikes) - min(chain.strikes)
    relative_width = strike_range / stock_price
    
    # Heuristik: breitere Range = höhere IV
    # Typische IV: 0.2-0.6 für Aktien
    estimated_iv = 0.2 + relative_width * 2
    return max(0.15, min(0.8, estimated_iv))

def get_strike_range_for_delta(chain, stock_price, expiration, min_delta=-0.50, max_delta=-0.10, r=0.05):
    """
    Berechnet Strike-Range für gewünschten Delta-Bereich mittels BS-Approximation
    Returns: (min_strike, max_strike) oder (None, None) wenn nicht möglich
    """
    # Zeit bis Expiration in Jahren
    try:
        exp_date = datetime.strptime(expiration, '%Y%m%d')
        T = (exp_date - datetime.now()).days / 365.0
        if T <= 0:
            return None, None
    except:
        return None, None
    
    # ATM IV schätzen
    sigma = estimate_atm_iv(chain, stock_price)
    logger.info(f"Geschätzte ATM IV: {sigma:.2%}, T={T:.3f} Jahre")
    
    # Für jeden Strike Delta berechnen und Range finden
    valid_strikes = []
    for strike in chain.strikes:
        delta = bs_put_delta(stock_price, strike, T, 0.05, sigma)
        if delta is not None and min_delta <= delta <= max_delta:
            valid_strikes.append(strike)
    
    if not valid_strikes:
        logger.warning(f"Keine Strikes im Delta-Bereich {min_delta} bis {max_delta} gefunden")
        return None, None
    
    min_strike = min(valid_strikes)
    max_strike = max(valid_strikes)
    logger.info(f"Delta-Filter: {len(valid_strikes)} Strikes im Bereich {min_strike}-{max_strike}")
    return min_strike, max_strike


def connect_ib(client_id=1):
    ib = IB()
    ib.connect('127.0.0.1', 7496, clientId=client_id, timeout=15, readonly=True)
    ib.reqMarketDataType(4)  # delayed-frozen for paper
    return ib


def get_defaults(symbol):
    return SYMBOL_DEFAULTS.get(symbol.upper(), {'exchange': 'SMART', 'currency': 'USD'})


def get_stock_and_chain(ib, symbol, month_index=0, exchange='SMART', currency='USD'):
    stock = Stock(symbol, exchange, currency)
    ib.qualifyContracts(stock)
    
    chains = ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
    
    # For European stocks, prefer EUREX for options
    option_exchange = exchange
    if currency == 'EUR':
        # Prefer EUREX for European options
        eurex_chains = [c for c in chains if c.exchange == 'EUREX' and c.expirations]
        if eurex_chains:
            chain = eurex_chains[0]
            option_exchange = 'EUREX'
        else:
            chain = [c for c in chains if c.expirations][0]
    else:
        # US stocks: use SMART or specified exchange
        chain = [c for c in chains if c.exchange == exchange]
        if not chain:
            chain = [c for c in chains if c.expirations]
        chain = chain[0]
    
    if month_index >= len(chain.expirations):
        raise ValueError(f"Month index {month_index} out of range (max {len(chain.expirations)-1})")
    
    expiration = chain.expirations[month_index]
    logger.info(f"Chain: exchange={chain.exchange}, tradingClass={chain.tradingClass}, Expiration: {expiration}, Strikes: {len(chain.strikes)}")
    return stock, chain, expiration, option_exchange


def get_stock_data(ib, stock):
    ticker = ib.reqMktData(stock, '', False, False)
    ib.sleep(2)
    data = {
        'conid': stock.conId, 'symbol': stock.symbol,
        'bid': ticker.bid, 'ask': ticker.ask, 'last': ticker.last,
        'close': ticker.close, 'volume': ticker.volume,
        'high': ticker.high, 'low': ticker.low,
    }
    ib.cancelMktData(stock)
    return data


def get_valid_put_options(ib, symbol, expiration, chain, exchange='SMART', min_strike=None, max_strike=None):
    """Create put options, optionally filtered by strike range, qualify, keep valid."""
    options = []
    for strike in chain.strikes:
        if min_strike is not None and strike < min_strike:
            continue
        if max_strike is not None and strike > max_strike:
            continue
        opt = Option(symbol, expiration, strike, 'P', exchange, tradingClass=chain.tradingClass, multiplier=chain.multiplier)
        options.append(opt)
    
    ib.qualifyContracts(*options)
    valid = [o for o in options if o.conId and o.conId > 0]
    logger.info(f"Qualified {len(valid)} of {len(options)} put options for {expiration}")
    return valid


def fetch_option_greeks_batch(ib, options):
    """Batch: subscribe all, wait once, collect all, cancel all."""
    if not options:
        return []
    
    # Subscribe all
    for opt in options:
        ib.reqMktData(opt, '', False, False)
    
    # Single wait for all
    ib.sleep(3)
    
    results = []
    for opt in options:
        ticker = ib.ticker(opt)
        
        greeks = ticker.modelGreeks
        row = {
            'conid': opt.conId, 'symbol': opt.symbol, 'right': opt.right,
            'expiration': opt.lastTradeDateOrContractMonth, 'strike': opt.strike,
            'multiplier': opt.multiplier,
            'bid': ticker.bid, 'ask': ticker.ask, 'last': ticker.last,
            'close': ticker.close, 'volume': ticker.volume,
            'bidSize': ticker.bidSize, 'askSize': ticker.askSize,
            'high': ticker.high, 'low': ticker.low, 'openPrice': ticker.open,
            'openInterest': getattr(ticker, 'putOpenInterest', None) or getattr(ticker, 'openInterest', None),
        }
        
        if greeks:
            row['delta'] = greeks.delta
            row['gamma'] = greeks.gamma
            row['theta'] = greeks.theta
            row['vega'] = greeks.vega
            row['impliedVol'] = greeks.impliedVol
        else:
            row['delta'] = row['gamma'] = row['theta'] = row['vega'] = row['impliedVol'] = None
        
        results.append(row)
        ib.cancelMktData(opt)
    
    return results


def filter_by_delta(rows, min_delta=-0.50, max_delta=-0.10):
    filtered = [r for r in rows if r['delta'] is not None and min_delta <= r['delta'] <= max_delta]
    return filtered


def write_csv(rows, filepath):
    DOLLAR_FIELDS = {"bid", "ask", "last", "close", "bidSize", "askSize", "high", "low", "openPrice"}
    GREEK_FIELDS = {"delta", "gamma", "theta", "vega", "impliedVol"}
    PCT_FIELDS = {"put_profit"}
    
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for r in rows:
            out_row = {}
            for h in HEADERS:
                val = r.get(h)
                if h == "put_profit":
                    bid = r.get("bid")
                    strike = r.get("strike")
                    if (bid is not None and not math.isnan(bid) and bid > 0
                        and strike is not None and not math.isnan(strike) 
                        and strike != 0):
                        val = (bid / strike) * 100
                    else:
                        val = None
                
                if val is None or val == '':
                    out_row[h] = ''
                elif isinstance(val, float):
                    if h in {"bid", "ask", "last", "close", "bidSize", "askSize", "high", "low", "openPrice"}:
                        out_row[h] = f'{val:.2f}'
                    elif h in {"delta", "gamma", "theta", "vega", "impliedVol"}:
                        out_row[h] = f'{val:.6f}'
                    elif h == "put_profit":
                        out_row[h] = f'{val:.2f}%'
                    else:
                        out_row[h] = f'{val:.6f}'
                else:
                    out_row[h] = val
            writer.writerow(out_row)



def main():
    if len(sys.argv) < 2:
        print("Usage: python3 tws_pipeline.py <SYMBOL> [MONTH_INDEX] [EXCHANGE] [CURRENCY]")
        print("Examples:")
        print("  python3 tws_pipeline.py ALV 0 XETRA EUR      # Allianz (Germany)")
        print("  python3 tws_pipeline.py ASML 0 AEB EUR       # ASML (Netherlands)")
        print("  python3 tws_pipeline.py CROX 0 SMART USD     # Crocs (US)")
        sys.exit(1)
    
    symbol = sys.argv[1].upper()
    month_index = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    
    defaults = get_defaults(symbol)
    exchange = sys.argv[3] if len(sys.argv) > 3 else defaults['exchange']
    currency = sys.argv[4] if len(sys.argv) > 4 else defaults['currency']
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f"/home/hermes/tws_option_pipeline/src/tws_option_contracts_{symbol}_{timestamp}.csv"
    
    logger.info(f"Starting TWS pipeline for {symbol} (month_index={month_index}, exchange={exchange}, currency={currency})")
    
    ib = None
    try:
        ib = connect_ib()
        logger.info(f"Connected to TWS, server version: {ib.client.serverVersion()}")
        
        stock, chain, expiration, option_exchange = get_stock_and_chain(ib, symbol, month_index, exchange, currency)
        stock_data = get_stock_data(ib, stock)
        stock_price = stock_data['last'] or stock_data['close']
        logger.info(f"Stock: {stock.symbol} @ {stock_price} {currency}")
        
        # OPTIMIERUNG: Strike-Range via Black-Scholes Delta Approximation bestimmen
        min_strike, max_strike = get_strike_range_for_delta(chain, stock_price, expiration)
        
        # Nur relevante Strikes qualifizieren
        options = get_valid_put_options(ib, symbol, expiration, chain, option_exchange, min_strike, max_strike)
        logger.info(f"Fetching Greeks for {len(options)} pre-filtered options...")
        
        # Batch Request für alle
        rows = fetch_option_greeks_batch(ib, options)
        filtered = filter_by_delta(rows)
        logger.info(f"Delta filter (-0.50 to -0.10): {len(filtered)} of {len(rows)}")
        
        write_csv(filtered, csv_file)
        logger.info(f"CSV written: {csv_file}")
        
        # Print CSV content to console
        print(f"\n{'='*80}")
        print(f"CSV CONTENT: {csv_file}")
        print(f"{'='*80}")
        with open(csv_file, 'r') as f:
            print(f.read())
        
        print(f"\n=== RESULT: {len(filtered)} PUT options with delta -0.50 to -0.10 ===")
        print(f"File: {csv_file}")
        print(f"Underlying: {symbol} @ {stock_price} {currency}")
        print(f"Expiration: {expiration}")
        if min_strike and max_strike:
            print(f"Pre-filtered strike range: {min_strike} - {max_strike}")
        for r in filtered:
            print(f"  Strike {r['strike']}: delta={r['delta']:.4f}, bid={r['bid']}, ask={r['ask']}, vol={r['volume']}, OI={r.get('openInterest','N/A')}, iv={r['impliedVol']:.4f}, put_profit={r.get('put_profit','N/A')}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
    finally:
        if ib and ib.isConnected():
            ib.disconnect()


if __name__ == '__main__':
    main()