#!/usr/bin/env python3
# Multi-Symbol Batch Pipeline: Sequentiell mit IBKR (kein Threading-Problem)
# Usage: python3 multi_symbol_batch.py [SYMBOLS...] [MONTH_INDICES...] [EXCHANGE] [CURRENCY]

import sys
import csv
import logging
import math
import asyncio
import errno
import socket
import os
from datetime import datetime
from pathlib import Path
from contextlib import redirect_stderr, contextmanager
from io import StringIO

@contextmanager
def suppress_ib_stderr(verbose):
    """No-op when using logging suppression."""
    yield

try:
    from ib_insync import IB, Stock, Option, RequestError, FlexError
except ImportError as exc:  # pragma: no cover - optional dependency at runtime
    class _MissingIBInsyncDependency:
        def __init__(self, *args, **kwargs):
            raise ModuleNotFoundError(
                "ib_insync is required to run this script. Install it with: pip install ib-insync"
            ) from exc

    class IB(_MissingIBInsyncDependency):
        pass

    class Stock(_MissingIBInsyncDependency):
        pass

    class Option(_MissingIBInsyncDependency):
        pass

    class RequestError(ImportError):
        pass

    class FlexError(ImportError):
        pass

# Logging will be configured in main() after argument parsing
logger = logging.getLogger(__name__)


# --- Error Handling Utilities ---
def handle_ibkr_error(operation: str, e: Exception, logger, default_return=None):
    """Standardized IBKR error handling with logging."""
    if isinstance(e, asyncio.TimeoutError):
        logger.error(f"{operation}: Timeout - {e}")
    elif isinstance(e, ConnectionRefusedError):
        logger.error(f"{operation}: Connection refused - Is TWS/Gateway running on port 7496?")
    elif isinstance(e, ConnectionError):
        logger.error(f"{operation}: Connection error - {e}")
    elif isinstance(e, RequestError):
        logger.error(f"{operation}: IBKR request error - {e}")
    elif isinstance(e, FlexError):
        logger.error(f"{operation}: IBKR Flex error - {e}")
    elif isinstance(e, OSError):
        logger.error(f"{operation}: OS/Network error - {e}")
    else:
        logger.error(f"{operation}: Unexpected error - {type(e).__name__}: {e}")
    return default_return


def handle_file_error(operation: str, e: Exception, logger, default_return=None):
    """Standardized file I/O error handling."""
    if isinstance(e, PermissionError):
        logger.error(f"{operation}: Permission denied - {e}")
    elif isinstance(e, OSError) and e.errno == errno.ENOSPC:
        logger.error(f"{operation}: Disk full - {e}")
    elif isinstance(e, FileNotFoundError):
        logger.error(f"{operation}: Path not found - {e}")
    else:
        logger.error(f"{operation}: File error - {type(e).__name__}: {e}")
    return default_return

HEADERS = [
    "symbol", "expiration", "strike", "multiplier",
    "bid", "ask", "last", "close", "volume",
    "delta", "gamma", "theta", "vega", "impliedVol",
    "bidSize", "askSize", "high", "low", "openPrice", "openInterest",
    "put_profit"
]


def format_row_for_output(row: dict) -> dict:
    """Format a row for CSV/HTML output with consistent precision."""
    out_row = {}
    for h in HEADERS:
        val = row.get(h)
        if h == 'put_profit':
            val = f'{val:.2f}%' if val is not None else ''
        elif isinstance(val, float):
            if h in {'bid', 'ask', 'last', 'close', 'bidSize', 'askSize', 'high', 'low', 'openPrice'}:
                val = f'{val:.2f}'
            elif h in {'delta', 'gamma', 'theta', 'vega', 'impliedVol'}:
                val = f'{val:.6f}'
            else:
                val = f'{val:.6f}'
        out_row[h] = val if val is not None else ''
    return out_row


def create_put_option(symbol: str, expiration: str, strike: float, multiplier: str,
                      exchange: str, trading_class: str) -> Option:
    """Create a PUT option contract with consistent parameters."""
    return Option(symbol, expiration, strike, 'P', exchange,
                  tradingClass=trading_class, multiplier=multiplier)


def get_mark_price(ib: IB, option: Option) -> float | None:
    """Get mark price for an option using fallback chain: markPrice -> mid -> close -> bid."""
    try:
        ib.reqMktData(option, '', False, False)
        try:
            ib.sleep(2)
        except Exception as e:
            logger.warning(f"get_mark_price: sleep interrupted - {e}")
    except RequestError as e:
        logger.error(f"get_mark_price: reqMktData failed - {e}")
        return None

    ticker = ib.ticker(option)
    if ticker is None:
        try:
            ib.cancelMktData(option)
        except Exception:
            pass
        return None

    mark_price = None
    if hasattr(ticker, 'markPrice') and ticker.markPrice and not math.isnan(ticker.markPrice):
        mark_price = ticker.markPrice
    elif ticker.bid and ticker.ask and ticker.bid > 0 and ticker.ask > 0:
        mark_price = (ticker.bid + ticker.ask) / 2
    elif ticker.close and ticker.close > 0:
        mark_price = ticker.close
    else:
        mark_price = ticker.bid

    try:
        ib.cancelMktData(option)
    except Exception:
        pass

    return mark_price


SYMBOL_DEFAULTS = {
    'ALV':   {'exchange': 'XETRA', 'currency': 'EUR'},
    'ASML':  {'exchange': 'AEB',   'currency': 'EUR'},
    'SAP':   {'exchange': 'XETRA', 'currency': 'EUR'},
    'SIE':   {'exchange': 'XETRA', 'currency': 'EUR'},
    'VOW3':  {'exchange': 'XETRA', 'currency': 'EUR'},
    'BMW':   {'exchange': 'XETRA', 'currency': 'EUR'},
    'DTE':   {'exchange': 'XETRA', 'currency': 'EUR'},
    'BAYN':  {'exchange': 'XETRA', 'currency': 'EUR'},
    'BAS':   {'exchange': 'XETRA', 'currency': 'EUR'},
    'DHL':   {'exchange': 'XETRA', 'currency': 'EUR'},
    'CROX':  {'exchange': 'SMART', 'currency': 'USD'},
    'TREX':  {'exchange': 'SMART', 'currency': 'USD'},
    'AAPL':  {'exchange': 'SMART', 'currency': 'USD'},
    'MSFT':  {'exchange': 'SMART', 'currency': 'USD'},
    'TSLA':  {'exchange': 'SMART', 'currency': 'USD'},
    'NVDA':  {'exchange': 'SMART', 'currency': 'USD'},
    'AMD':   {'exchange': 'SMART', 'currency': 'USD'},
    'SPY':   {'exchange': 'SMART', 'currency': 'USD'},
    'QQQ':   {'exchange': 'SMART', 'currency': 'USD'},
}

# Black-Scholes Delta Approximation
def bs_put_delta(S, K, T, r, sigma):
    try:
        if sigma <= 0 or T <= 0:
            return None
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        delta = 0.5 * (1 + math.erf(d1 / math.sqrt(2))) - 1
        return delta
    except (ZeroDivisionError, ValueError, OverflowError) as e:
        logger.warning(f"bs_put_delta: Math error - {type(e).__name__}: {e}")
        return None
    except Exception as e:
        handle_ibkr_error("bs_put_delta", e, logger)
        return None

def estimate_atm_iv(chain, stock_price):
    try:
        strikes = getattr(chain, 'strikes', None)
        if not strikes:
            logger.warning("estimate_atm_iv: chain.strikes is empty or missing")
            return 0.3
        atm_strike = min(strikes, key=lambda x: abs(x - stock_price))
        strike_range = max(strikes) - min(strikes)
        relative_width = strike_range / stock_price
        estimated_iv = 0.2 + relative_width * 2
        return max(0.15, min(0.8, estimated_iv))
    except (ValueError, ZeroDivisionError, AttributeError) as e:
        logger.warning(f"estimate_atm_iv: Calculation error - {type(e).__name__}: {e}")
        return 0.3
    except Exception as e:
        handle_ibkr_error("estimate_atm_iv", e, logger, 0.3)
        return 0.3

def get_strike_range_for_delta(chain, stock_price, expiration, min_delta=-0.50, max_delta=-0.10, r=0.05):
    """Returns list of strikes with BS delta in range, not just min/max.
    This avoids requesting market data for strikes that won't have quotes."""
    try:
        exp_date = datetime.strptime(expiration, '%Y%m%d')
        T = (exp_date - datetime.now()).days / 365.0
        if T <= 0:
            return []
    except Exception as e:
        logger.warning(f"get_strike_range_for_delta: Date parse error - {type(e).__name__}: {e}")
        return []

    try:
        sigma = estimate_atm_iv(chain, stock_price)
        logger.info(f"  {expiration}: est. IV={sigma:.2%}, T={T:.3f}y")

        valid_strikes = []
        for strike in chain.strikes:
            delta = bs_put_delta(stock_price, strike, T, 0.05, sigma)
            if delta is not None and min_delta <= delta <= max_delta:
                valid_strikes.append(strike)

        if not valid_strikes:
            logger.warning(f"  No strikes in delta range {min_delta} to {max_delta}")
            return []
        
        logger.info(f"  Delta-filtered strikes: {valid_strikes}")
        return valid_strikes
    except Exception as e:
        handle_ibkr_error("get_strike_range_for_delta", e, logger)
        return []


def connect_ib(client_id, verbose=False):
    try:
        ib = IB()
        
        # Custom error handler to filter informational codes
        if not verbose:
            # Informational codes that don't need to be shown
            INFO_CODES = {10090, 10167, 2104, 2106, 2158, 2103, 2105, 2107, 2157}
            def filter_errors(req_id, error_code, error_string, contract):
                if error_code not in INFO_CODES:
                    logger.error(f"IBKR Error {error_code}: {error_string}")
            ib.errorEvent += filter_errors
        
        # Suppress stderr from C++ wrapper (ERROR 10090, 2104, etc.)
        if not verbose:
            stderr_capture = StringIO()
            with redirect_stderr(stderr_capture):
                ib.connect('127.0.0.1', 7496, clientId=client_id, timeout=15, readonly=True)
                ib.reqMarketDataType(4)
        else:
            ib.connect('127.0.0.1', 7496, clientId=client_id, timeout=15, readonly=True)
            ib.reqMarketDataType(4)
        return ib
    except asyncio.TimeoutError:
        logger.error(f"connect_ib: Connection timeout (clientId={client_id})")
    except ConnectionRefusedError:
        logger.error("connect_ib: Connection refused - Is TWS/Gateway running on port 7496?")
    except ConnectionError as e:
        logger.error(f"connect_ib: Connection error - {e}")
    except RequestError as e:
        logger.error(f"connect_ib: IBKR request error - {e}")
    except Exception as e:
        handle_ibkr_error("connect_ib", e, logger)
    return None


def get_defaults(symbol):
    return SYMBOL_DEFAULTS.get(symbol.upper(), {'exchange': 'SMART', 'currency': 'USD'})


def get_stock_and_chain(ib, symbol, month_index, exchange, currency):
    try:
        stock = Stock(symbol, exchange, currency)
        ib.qualifyContracts(stock)

        chains = ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
        chain = [c for c in chains if c.exchange == exchange]
        if not chain:
            chain = [c for c in chains if c.expirations]
            if not chain:
                raise ValueError(f"No option chain for {symbol} on {exchange}")
            chain = chain[0]
        else:
            chain = chain[0]

        expirations = getattr(chain, 'expirations', None)
        if not expirations:
            raise ValueError(f"No expirations available for {symbol} on {exchange}")

        if month_index >= len(expirations):
            raise ValueError(f"Month index {month_index} out of range (max {len(expirations) - 1})")

        expiration = expirations[month_index]
        return stock, chain, expiration
    except ValueError:
        raise  # Re-raise validation errors
    except RequestError as e:
        logger.error(f"get_stock_and_chain({symbol}): IBKR request error - {e}")
    except Exception as e:
        handle_ibkr_error(f"get_stock_and_chain({symbol})", e, logger)
    return None, None, None


def get_stock_data(ib, stock, timeout: float = 5.0):
    """Get stock market data with polling instead of fixed sleep."""
    try:
        ticker = ib.reqMktData(stock, '', False, False)
        start_time = datetime.now()
        while (datetime.now() - start_time).total_seconds() < timeout:
            try:
                ib.sleep(0.5)
            except Exception as e:
                logger.warning(f"get_stock_data: sleep interrupted - {e}")
            # Check if we have meaningful data (last > 0, not just != None since IB returns -1.0 as placeholder)
            if (ticker.last is not None and ticker.last > 0) or ticker.close is not None or (ticker.bid is not None and ticker.ask is not None):
                break
        data = {
            'conid': stock.conId, 'symbol': stock.symbol,
            'bid': ticker.bid, 'ask': ticker.ask, 'last': ticker.last if ticker.last and ticker.last > 0 else None,
            'close': ticker.close, 'volume': ticker.volume,
            'high': ticker.high, 'low': ticker.low,
        }
        try:
            ib.cancelMktData(stock)
        except Exception:
            pass  # Best effort cleanup
        return data
    except RequestError as e:
        logger.error(f"get_stock_data({stock.symbol}): IBKR request error - {e}")
    except Exception as e:
        handle_ibkr_error(f"get_stock_data({stock.symbol})", e, logger)
    return None


def get_valid_put_options(ib, symbol, expiration, chain, exchange, min_strike, max_strike, stock_price=None, valid_strikes=None):
    try:
        options = []
        # If valid_strikes provided, use those directly (from BS delta filter)
        # Otherwise iterate all chain.strikes with min/max/moneyness filters
        strikes_to_use = valid_strikes if valid_strikes is not None else chain.strikes
        
        # Filter: nur 5-Punkt-Strikes (liquide US-Standard), keine 2.5-Punkt (existieren oft nicht)
        strikes_to_use = [s for s in strikes_to_use if abs(s % 5) < 0.01]
        
        # Begrenzen auf max 5 liquideste Strikes (nahe ATM) für Paper Delayed Data
        # Sortiere nach Moneyness (näher an 1.0 = ATM)
        if stock_price is not None and len(strikes_to_use) > 5:
            strikes_to_use.sort(key=lambda s: abs(s / stock_price - 1.0))
            strikes_to_use = strikes_to_use[:5]
            logger.info(f"  Limited to top 5 ATM strikes for market data: {strikes_to_use}")
        
        for strike in strikes_to_use:
            if min_strike is not None and strike < min_strike:
                continue
            if max_strike is not None and strike > max_strike:
                continue
            # Filter: nur Strikes in sinnvollem Moneyness-Bereich (70%-130% von Spot)
            # Paper Delayed Data liefert nur für liquide Strikes nahe ATM Bids
            if stock_price is not None:
                moneyness = strike / stock_price
                if moneyness < 0.70 or moneyness > 1.30:
                    continue
            opt = Option(symbol, expiration, strike, 'P', exchange, tradingClass=chain.tradingClass, multiplier=chain.multiplier)
            options.append(opt)

        ib.qualifyContracts(*options)
        valid = [o for o in options if o.conId and o.conId > 0]
        return valid
    except RequestError as e:
        logger.error(f"get_valid_put_options({symbol}): IBKR request error - {e}")
    except Exception as e:
        handle_ibkr_error(f"get_valid_put_options({symbol})", e, logger)
    return []


def fetch_option_greeks_batch(ib, options, verbose=False):
    """Batch: subscribe all, wait once, collect all, cancel all. (wie tws_pipeline.py)"""
    if not options:
        return []

    # Subscribe all (suppress stderr for 10090 errors)
    with suppress_ib_stderr(verbose):
        for opt in options:
            try:
                ib.reqMktData(opt, '', False, False)
            except Exception as e:
                logger.warning(f"fetch_option_greeks_batch: reqMktData failed for {opt.localSymbol}: {e}")

    # Single wait for all (3s wie in tws_pipeline.py)
    try:
        ib.sleep(3)
    except Exception as e:
        logger.warning(f"fetch_option_greeks_batch: sleep interrupted: {e}")

    results = []
    for opt in options:
        try:
            ticker = ib.ticker(opt)
            if ticker is None:
                logger.warning(f"fetch_option_greeks_batch: No ticker for {opt.localSymbol}")
                continue
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
        except Exception as e:
            logger.warning(f"fetch_option_greeks_batch: Failed for {opt.localSymbol}: {e}")
        finally:
            try:
                ib.cancelMktData(opt)
            except Exception:
                pass  # Best effort cleanup

    return results


def filter_by_delta(rows, min_delta=-0.50, max_delta=-0.10):
    return [r for r in rows if r['delta'] is not None and min_delta <= r['delta'] <= max_delta]


def check_margin(ib, option, quantity=1, price=None):
    """
    Prüft ob genügend Margin für Short PUT verfügbar ist.
    Verwendet whatIfOrder für exakte Margin-Berechnung.
    Returns: (ok: bool, required_margin: float, available: float, msg: str)
    """
    from ib_insync import Order
    
    # Account Summary holen
    acct = ib.accountSummary()
    available = 0.0
    for v in acct:
        if v.tag == 'AvailableFunds':
            available = float(v.value)
            break
    
    if available <= 0:
        return False, 0, 0, "Keine AvailableFunds gefunden"
    
    # WhatIf Order für Margin-Check
    test_order = Order()
    test_order.action = 'SELL'
    test_order.orderType = 'LMT'
    test_order.totalQuantity = quantity
    test_order.lmtPrice = round(price, 2) if price else 0
    test_order.tif = 'DAY'
    test_order.whatIf = True  # WICHTIG: nur Simulierung
    
    try:
        whatif_trade = ib.placeOrder(option, test_order)
        ib.sleep(1.5)  # Warten auf Margin-Berechnung
        
        # Margin aus whatIf Trade lesen
        required = 0.0
        if whatif_trade.orderStatus.marginChange is not None:
            required = abs(whatif_trade.orderStatus.marginChange)
        elif whatif_trade.orderStatus.initMarginChange is not None:
            required = abs(whatif_trade.orderStatus.initMarginChange)
        elif whatif_trade.orderStatus.maintMarginChange is not None:
            required = abs(whatif_trade.orderStatus.maintMarginChange)
        
        # Fallback: Rough estimate für Short PUT (~20% von Strike * 100)
        if required == 0 and price:
            required = option.strike * 100 * 0.20
        
        # Safety buffer: nur 80% der verfügbaren Funds nutzen
        max_allowed = available * 0.80
        
        if required > max_allowed:
            return False, required, available, f"Margin ${required:,.0f} > 80% Available ${max_allowed:,.0f}"
        
        return True, required, available, f"OK: Margin ${required:,.0f} <= ${max_allowed:,.0f}"
        
    except Exception as e:
        # Fallback auf Schätzung
        est_required = option.strike * 100 * 0.20 if price else 0
        max_allowed = available * 0.80
        if est_required > max_allowed:
            return False, est_required, available, f"Est. Margin ${est_required:,.0f} > 80% Available ${max_allowed:,.0f}"
        return True, est_required, available, f"Est. OK: ${est_required:,.0f} <= ${max_allowed:,.0f}"


def place_option_order(ib, option, action='SELL', quantity=1, order_type='LMT', limit_price=None, auto_transmit=False, bracket=False, premium=None):
    """
    Platziert eine Option Order in TWS mit Bestätigung.
    bracket=True: Erstellt Bracket Order (Take Profit 70% / Stop Loss 100% Premium)
    premium: Premium für Bracket Berechnung (wenn None, wird limit_price/mid verwendet)
    Returns: True wenn Order platziert, False sonst.
    """
    try:
        from ib_insync import Order

        # Für REL Orders (Pegged-to-Midpoint): kein Limit-Preis, auxPrice=0 für Midpoint
        if order_type == 'REL':
            order_type = 'REL'
            limit_price = None  # REL orders haben keinen Limit-Preis
        else:
            order_type = 'LMT'
            if limit_price is None:
                # Zuerst Marktdaten anfordern, dann Bid/Ask lesen
                try:
                    ib.reqMktData(option, '', False, False)
                    try:
                        ib.sleep(2)  # Warten auf Marktdaten
                    except Exception as e:
                        logger.warning(f"place_option_order: sleep interrupted - {e}")
                except RequestError as e:
                    logger.error(f"place_option_order: reqMktData failed - {e}")
                    return False

                ticker = ib.ticker(option)
                if ticker is None:
                    logger.warning(f"Kein Ticker für {option.localSymbol}")
                    return False

                limit_price = ticker.bid if action == 'SELL' else ticker.ask
                try:
                    ib.cancelMktData(option)
                except Exception:
                    pass  # Best effort cleanup

            if limit_price is None or limit_price <= 0:
                logger.warning(f"Kein gültiger Limit-Preis für {option.localSymbol}")
                return False

        # MARGIN CHECK vor Auto-Order
        # Für REL Orders: Margin basierend auf Mid-Preis prüfen
        check_price = limit_price
        if order_type == 'REL' and check_price is None:
            # Mid-Preis für Margin-Check holen
            try:
                ib.reqMktData(option, '', False, False)
                try:
                    ib.sleep(2)
                except Exception as e:
                    logger.warning(f"place_option_order: sleep interrupted - {e}")
            except RequestError as e:
                logger.error(f"place_option_order: reqMktData for margin check failed - {e}")
                return False
            ticker = ib.ticker(option)
            if ticker.bid and ticker.ask and ticker.bid > 0 and ticker.ask > 0:
                check_price = (ticker.bid + ticker.ask) / 2
            else:
                check_price = ticker.bid or ticker.close
            try:
                ib.cancelMktData(option)
            except Exception:
                pass  # Best effort cleanup

        if auto_transmit and action == 'SELL' and option.right == 'P':
            ok, required, available, msg = check_margin(ib, option, quantity, check_price)
            print(f"🔍 Margin Check: {msg}")
            if not ok:
                logger.warning(f"Margin Check fehlgeschlagen: {msg}")
                print(f"❌ Order übersprungen: {msg}")
                return False

        # Bracket Order Logic
        if bracket and action == 'SELL' and option.right == 'P':
            # Premium für Bracket berechnen
            if premium is None:
                premium = limit_price if limit_price else check_price
            
            take_profit_price = round(premium * 0.30, 2)  # 70% profit = buy back at 30%
            stop_loss_price = round(premium * 2.00, 2)    # 100% loss = buy back at 200%
            
            print(f"\n📊 BRACKET ORDER:")
            print(f"   Premium: ${premium:.2f}")
            print(f"   Take Profit (70%): BUY @ ${take_profit_price:.2f}")
            print(f"   Stop Loss (100%): BUY @ ${stop_loss_price:.2f}")
            
            return place_bracket_order(ib, option, quantity, order_type, limit_price, 
                                       take_profit_price, stop_loss_price, auto_transmit)

        # Normal Order (existing logic)
        order = Order()
        order.action = action
        order.orderType = order_type
        order.totalQuantity = quantity
        if order_type == 'LMT':
            order.lmtPrice = round(limit_price, 2)
        elif order_type == 'REL':
            order.auxPrice = 0  # Offset 0 = Midpoint
        order.tif = 'DAY'  # Explizit DAY setzen um Error 10349 zu vermeiden

        if auto_transmit:
            try:
                trade = ib.placeOrder(option, order)
                try:
                    ib.sleep(1)
                except Exception as e:
                    logger.warning(f"place_option_order: sleep interrupted - {e}")
                logger.info(f"Auto-Order platziert: {trade.orderStatus.status}")
                print(f"✅ Auto-Order gesendet: {option.localSymbol} @ ${limit_price:.2f} ({trade.orderStatus.status})")
                return True
            except RequestError as e:
                logger.error(f"place_option_order: placeOrder failed - {e}")
                return False
            except Exception as e:
                handle_ibkr_error(f"place_option_order(auto)", e, logger)
                return False
        else:
            order.transmit = False  # Nicht sofort senden

            # Trade Objekt erstellen (aber nicht senden)
            try:
                trade = ib.placeOrder(option, order)
            except RequestError as e:
                logger.error(f"place_option_order: placeOrder (prepare) failed - {e}")
                return False
            except Exception as e:
                handle_ibkr_error(f"place_option_order(prepare)", e, logger)
                return False

            # Bestätigung anfordern
            print(f"\n{'='*60}")
            print(f"ORDER BESTÄTIGUNG")
            print(f"{'='*60}")
            print(f"Symbol:     {option.symbol}")
            print(f"Expiration: {option.lastTradeDateOrContractMonth}")
            print(f"Strike:     {option.strike}")
            print(f"Right:      {option.right}")
            print(f"Action:     {action}")
            print(f"Quantity:   {quantity}")
            print(f"OrderType:  {order_type}")
            if order_type == 'LMT' and limit_price is not None:
                print(f"LimitPrice: ${limit_price:.2f}")
            elif order_type == 'REL':
                print(f"LimitPrice: REL (Pegged-to-Midpoint, Offset=0)")
            else:
                print(f"LimitPrice: MID (Market Mid-Price)")
            print(f"Contract:   {option.localSymbol}")
            
            # Margin Check für Short PUT
            if action == 'SELL' and option.right == 'P':
                # Verfügbare Funds holen
                acct = ib.accountSummary()
                available = 0.0
                for v in acct:
                    if v.tag == 'AvailableFunds':
                        available = float(v.value)
                        break
                
                if available > 0:
                    # Margin Check mit whatIfOrder
                    from ib_insync import Order
                    test_order = Order()
                    test_order.action = 'SELL'
                    test_order.orderType = 'LMT'
                    test_order.totalQuantity = quantity
                    test_order.lmtPrice = round(limit_price, 2) if limit_price else 0
                    test_order.tif = 'DAY'
                    test_order.whatIf = True
                    
                    try:
                        whatif_trade = ib.placeOrder(option, test_order)
                        ib.sleep(1.5)
                        required = 0.0
                        if whatif_trade.orderStatus.marginChange is not None:
                            required = abs(whatif_trade.orderStatus.marginChange)
                        elif whatif_trade.orderStatus.initMarginChange is not None:
                            required = abs(whatif_trade.orderStatus.initMarginChange)
                        elif whatif_trade.orderStatus.maintMarginChange is not None:
                            required = abs(whatif_trade.orderStatus.maintMarginChange)
                        if required == 0 and limit_price:
                            required = option.strike * 100 * 0.20
            
                        margin_pct = (required / available * 100) if available > 0 else 0
                        max_allowed = available * 0.80
                        ok = required <= max_allowed
            
                        print(f"Margin:     ${required:,.2f} / ${available:,.2f} ({margin_pct:.1f}%)")
                        print(f"Max 80%:    ${max_allowed:,.2f} ({'OK' if ok else 'ÜBERSCHRITTEN'})")
                    except Exception as e:
                        logger.warning(f"Margin-Check fehlgeschlagen: {e}")
                        # Fallback Schätzung
                        est_required = option.strike * 100 * 0.20
                        margin_pct = (est_required / available * 100) if available > 0 else 0
                        max_allowed = available * 0.80
                        ok = est_required <= max_allowed
                        print(f"Margin(est): ${est_required:,.2f} / ${available:,.2f} ({margin_pct:.1f}%)")
                        print(f"Max 80%:     ${max_allowed:,.2f} ({'OK' if ok else 'ÜBERSCHRITTEN'})")
                else:
                    print(f"Margin:     Nicht verfügbar")
            
            # Bracket Order Auswahl
            use_bracket = False
            tp_pct = 0.30  # 70% profit = 30% of premium
            sl_pct = 2.00  # 100% loss = 200% of premium
            if bracket and action == 'SELL' and option.right == 'P' and limit_price:
                print(f"\nBracket Order Option:")
                print(f"  Take Profit: 70% (Buy back @ 30% = ${limit_price * 0.30:.2f})")
                print(f"  Stop Loss:   100% (Buy back @ 200% = ${limit_price * 2.00:.2f})")
                print(f"  Standard:    70% / 100% (y/n/c für Custom): ", end="")
                
                try:
                    bracket_choice = input().strip().lower()
                    if bracket_choice in ['y', 'yes', 'j', 'ja']:
                        use_bracket = True
                    elif bracket_choice in ['c', 'custom']:
                        try:
                            tp_input = input("  Take Profit % (z.B. 70): ").strip()
                            sl_input = input("  Stop Loss % (z.B. 100): ").strip()
                            tp_pct = 1.0 - float(tp_input) / 100  # 70% -> 0.30
                            sl_pct = float(sl_input) / 100 * 2  # 100% -> 2.0
                            use_bracket = True
                        except:
                            print("  Ungültig, verwende Standard 70%/100%")
                            use_bracket = True
                except:
                    pass
            elif bracket and action == 'SELL' and option.right == 'P' and limit_price:
                use_bracket = True
            
            print(f"{'='*60}")
            
            while True:
                try:
                                        confirm = input("Order platzieren? (y/n): ").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    logger.info("place_option_order: User cancelled input")
                    try:
                        ib.cancelOrder(trade.order)
                    except Exception:
                        pass
                        return False
                if confirm in ['y', 'yes', 'j', 'ja']:
                    try:
                        trade.transmit = True
                        # Warten bis Order einen finalen Status erreicht
                        final_statuses = {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive', 'Submitted'}
                        for _ in range(10):  # Max 10 Sekunden warten
                            ib.sleep(1)
                            status = trade.orderStatus.status
                            if status in final_statuses:
                                break
                        logger.info(f"Order platziert: {trade.orderStatus.status}")
                        print(f"Order Status: {trade.orderStatus.status}")
                        return True
                    except Exception as e:
                        handle_ibkr_error("place_option_order(transmit)", e, logger)
                    return False
                elif confirm in ['n', 'no', 'nein']:
                    try:
                        ib.cancelOrder(trade.order)
                    except Exception:
                        pass
                    logger.info("Order abgebrochen")
                    return False
                else:
                    print("Bitte 'y' oder 'n' eingeben.")
    except RequestError as e:
        logger.error(f"place_option_order: IBKR request error - {e}")
    except ConnectionError as e:
        logger.error(f"place_option_order: Connection lost - {e}")
    except Exception as e:
        handle_ibkr_error("place_option_order", e, logger)
    return False


def place_bracket_order(ib, option, quantity, order_type, limit_price, take_profit_price, stop_loss_price, auto_transmit=False):
    """
    Erstellt eine OCA Group Order für Short PUT:
    - Parent: SELL PUT @ limit_price (oder REL)
    - Take Profit: BUY @ take_profit_price (30% of premium = 70% profit)
    - Stop Loss: BUY @ stop_loss_price (200% of premium = 100% loss)
    OCA Group: One-Cancels-All (ocaType=1)
    """
    from ib_insync import Order
    import uuid
    
    # Eindeutige OCA Group ID
    oca_group = f"OCA_{option.symbol}_{option.lastTradeDateOrContractMonth}_{uuid.uuid4().hex[:8]}"
    
    # Parent Order (SELL PUT)
    parent = Order()
    parent.action = 'SELL'
    parent.orderType = order_type
    parent.totalQuantity = quantity
    parent.tif = 'DAY'
    parent.transmit = False
    
    if order_type == 'LMT':
        parent.lmtPrice = round(limit_price, 2)
    elif order_type == 'REL':
        parent.auxPrice = 0  # Midpoint
    
    # OCA Group für Parent
    parent.ocaGroup = oca_group
    parent.ocaType = 1  # 1 = Cancel all remaining when one fills
    
    # Take Profit Order (BUY back at 30% of premium)
    tp_order = Order()
    tp_order.action = 'BUY'
    tp_order.orderType = 'LMT'
    tp_order.totalQuantity = quantity
    tp_order.lmtPrice = take_profit_price
    tp_order.tif = 'DAY'
    tp_order.transmit = False
    tp_order.ocaGroup = oca_group
    tp_order.ocaType = 1
    
    # Stop Loss Order (BUY back at 200% of premium)
    sl_order = Order()
    sl_order.action = 'BUY'
    sl_order.orderType = 'STP'
    sl_order.totalQuantity = quantity
    sl_order.auxPrice = stop_loss_price  # Stop price
    sl_order.tif = 'DAY'
    sl_order.transmit = False
    sl_order.ocaGroup = oca_group
    sl_order.ocaType = 1
    
    # Place all three orders
    print(f"📊 OCA Group Order platzieren:")
    print(f"   Group: {oca_group}")
    print(f"   Parent: SELL {quantity} {option.localSymbol} @ {order_type} {limit_price if limit_price else 'REL'}")
    print(f"   Take Profit: BUY {quantity} @ ${take_profit_price:.2f} LMT (70% profit)")
    print(f"   Stop Loss: BUY {quantity} @ ${stop_loss_price:.2f} STP (100% loss)")
    print(f"   OCA Type: 1 (One-Cancels-All)")
    
    try:
        # Place all three orders rapidly (no sleep between)
        # Only the LAST order transmits = all 3 sent atomar
        parent_trade = ib.placeOrder(option, parent)
        tp_trade = ib.placeOrder(option, tp_order)
        
        # Last order transmits all
        sl_order.transmit = True
        sl_trade = ib.placeOrder(option, sl_order)
        
        # Brief wait for confirmation
        ib.sleep(1)
        
        logger.info(f"OCA Group Order platziert: Parent={parent_trade.orderStatus.status}, TP={tp_trade.orderStatus.status}, SL={sl_trade.orderStatus.status}, Group={oca_group}")
        print(f"✅ OCA Group platziert: Parent={parent_trade.orderStatus.status}, TP={tp_trade.orderStatus.status}, SL={sl_trade.orderStatus.status}")
        print(f"   OCA Group: {oca_group} (Type 1: One-Cancels-All)")
        return True
        
    except Exception as e:
        handle_ibkr_error("place_bracket_order", e, logger)
        return False


def place_spread_order(ib, short_option, long_option, quantity, order_type, credit_limit_price, auto_transmit=False, verbose=False):
    """
    Platziert einen Bull Put Spread (Credit Spread):
    - Short Leg: SELL PUT @ higher strike
    - Long Leg:  BUY PUT @ lower strike
    OCA Group für beide Legs (One-Cancels-All)
    """
    from ib_insync import Order
    import uuid
    
    # OCA Group für beide Legs
    oca_group = f"SPREAD_{short_option.symbol}_{short_option.lastTradeDateOrContractMonth}_{uuid.uuid4().hex[:8]}"
    
    # Short Leg (SELL Higher Strike PUT)
    short_order = Order()
    short_order.action = 'SELL'
    short_order.orderType = order_type
    short_order.totalQuantity = quantity
    short_order.tif = 'DAY'
    short_order.transmit = False
    short_order.ocaGroup = oca_group
    short_order.ocaType = 1
    
    # Long Leg (BUY Lower Strike PUT)
    long_order = Order()
    long_order.action = 'BUY'
    long_order.orderType = order_type
    long_order.totalQuantity = quantity
    long_order.tif = 'DAY'
    long_order.transmit = False
    long_order.ocaGroup = oca_group
    long_order.ocaType = 1
    
    # Est. Credit für Display
    est_credit = (short_option.bid if hasattr(short_option, 'bid') and short_option.bid else 0) -                  (long_option.ask if hasattr(long_option, 'ask') and long_option.ask else 0)
    
    print(f"📊 BULL PUT SPREAD Order platzieren:")
    print(f"   Group: {oca_group}")
    print(f"   Short: SELL {quantity} {short_option.localSymbol} @ {order_type}")
    print(f"   Long:  BUY  {quantity} {long_option.localSymbol} @ {order_type}")
    print(f"   Est. Credit: ${est_credit:.2f} | Width: ${short_option.strike - long_option.strike:.0f}")
    print(f"   OCA Type: 1 (One-Cancels-All)")
    
    try:
        # Market Data für beide Legs anfordern für aktuelle Preise
        with suppress_ib_stderr(verbose):
            ib.reqMktData(short_option, '', False, False)
            ib.reqMktData(long_option, '', False, False)
            ib.sleep(1)
        
        short_ticker = ib.ticker(short_option)
        long_ticker = ib.ticker(long_option)
        
        short_bid = short_ticker.bid if short_ticker and short_ticker.bid else 0
        short_ask = short_ticker.ask if short_ticker and short_ticker.ask else 0
        long_bid = long_ticker.bid if long_ticker and long_ticker.bid else 0
        long_ask = long_ticker.ask if long_ticker and long_ticker.ask else 0
        
        ib.cancelMktData(short_option)
        ib.cancelMktData(long_option)
        
        if short_bid <= 0 or long_ask <= 0:
            print(f"❌ Keine gültigen Preise: Short Bid={short_bid}, Long Ask={long_ask}")
            return False
        
        # Orders mit aktuellen Preisen aktualisieren
        if order_type == 'LMT':
            short_order.lmtPrice = round(short_bid, 2)
            long_order.lmtPrice = round(long_ask, 2)
        
        net_credit = short_bid - long_ask
        if net_credit <= 0:
            print(f"❌ Kein Credit Spread möglich: Short Bid={short_bid}, Long Ask={long_ask}")
            return False
        
        print(f"   Live Credit: ${net_credit:.2f} (Short Bid ${short_bid:.2f} - Long Ask ${long_ask:.2f})")
        
        # Place both orders (last transmits)
        short_trade = ib.placeOrder(short_option, short_order)
        long_order.transmit = True
        long_trade = ib.placeOrder(long_option, long_order)
        
        ib.sleep(1)
        
        logger.info(f"Bull Put Spread platziert: Short={short_trade.orderStatus.status}, Long={long_trade.orderStatus.status}, Group={oca_group}")
        print(f"✅ Bull Put Spread platziert: Short={short_trade.orderStatus.status}, Long={long_trade.orderStatus.status}")
        print(f"   OCA Group: {oca_group} (Type 1: One-Cancels-All)")
        print(f"   Net Credit: ${net_credit:.2f} | Max Risk: ${(short_option.strike - long_option.strike) * 100 - net_credit * 100:.0f}")
        return True
        
    except Exception as e:
        handle_ibkr_error("place_spread_order", e, logger)
        return False


def write_master_csv(results, filepath):
    try:
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS)
            writer.writeheader()
            for res in results:
                if 'error' in res:
                    continue
                for r in res['top5']:
                    out_row = format_row_for_output(r)
                    writer.writerow(out_row)
        logger.info(f"Master CSV written: {filepath}")
        return True
    except PermissionError:
        logger.error(f"write_master_csv: Permission denied - {filepath}")
    except OSError as e:
        if e.errno == errno.ENOSPC:
            logger.error(f"write_master_csv: Disk full - {filepath}")
        else:
            logger.error(f"write_master_csv: OS error - {e}")
    except Exception as e:
        handle_file_error("write_master_csv", e, logger)
    return False


def process_symbol(symbol, month_indices, exchange, currency, client_id, keep_connected=False, delta_min=-0.50, delta_max=-0.10, num_chains=0, order_type='LMT', bid_min=0.80, bracket_mode=False, spread_mode=False, spread_width=5, max_margin=1000, min_credit=0.80, verbose=False):
    """Verarbeitet ein einzelnes Symbol"""
    ib = None
    try:
        ib = connect_ib(client_id, verbose=verbose)
        if ib is None:
            return {'symbol': symbol, 'error': f'Failed to connect to IBKR (clientId={client_id})'}

        # Paper Account: Delayed-Frozen Market Data (Type 4) aktivieren
        ib.reqMarketDataType(4)
        logger.info("Market data type set to 4 (delayed-frozen)")

        stock, chain, _ = get_stock_and_chain(ib, symbol, month_indices[0], exchange, currency)
        if stock is None:
            return {'symbol': symbol, 'error': 'Failed to get stock and chain'}

        stock_data = get_stock_data(ib, stock)
        if stock_data is None:
            return {'symbol': symbol, 'error': 'Failed to get stock data'}

        stock_price = stock_data['last'] or stock_data['close']
        logger.info(f"{symbol}: Stock @ {stock_price} {currency}")

        # If num_chains specified, get all available expirations and use first N
        effective_month_indices = month_indices
        if num_chains > 0:
            all_chains = ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
            chain = [c for c in all_chains if c.exchange == exchange]
            if not chain:
                chain = [c for c in all_chains if c.expirations]
            if chain:
                chain = chain[0]
                expirations = getattr(chain, 'expirations', [])
                if expirations:
                    available_expirations = expirations[:num_chains]
                    logger.info(f"  Using first {num_chains} of {len(expirations)} available chains: {available_expirations}")
                    effective_month_indices = list(range(len(available_expirations)))
                else:
                    logger.warning(f"  No expirations found for {symbol}")
                    available_expirations = []
            else:
                available_expirations = []

        all_rows = []

        for month_idx in effective_month_indices:
            try:
                _, chain, expiration = get_stock_and_chain(ib, symbol, month_idx, exchange, currency)
                if chain is None:
                    logger.warning(f"  {symbol} month_idx={month_idx}: Failed to get chain")
                    continue

                stock_price = stock_data['last'] or stock_data['close']

                valid_strikes = get_strike_range_for_delta(chain, stock_price, expiration, min_delta=delta_min, max_delta=delta_max)
                if not valid_strikes:
                    logger.warning(f"  {symbol} {expiration}: Keine Strikes im Delta-Bereich")
                    continue

                options = get_valid_put_options(ib, symbol, expiration, chain, exchange, min(valid_strikes), max(valid_strikes), stock_price=stock_price, valid_strikes=valid_strikes)
                if not options:
                    continue

                rows = fetch_option_greeks_batch(ib, options)
                logger.info(f"fetch_option_greeks_batch: {len(rows)} options returned, sample: {rows[:2] if rows else 'empty'}")
                filtered = filter_by_delta(rows, min_delta=delta_min, max_delta=delta_max)
                logger.info(f"filter_by_delta: {len(filtered)} options in delta range [{delta_min}, {delta_max}]")
                
                # Filter by minimum bid and max 28 days to expiration
                from datetime import datetime
                now = datetime.now()
                bid_filtered = []
                for r in filtered:
                    bid = r.get('bid')
                    logger.debug(f"Option {r.get('symbol')} {r.get('strike')} {r.get('expiration')}: bid={bid}, delta={r.get('delta')}")
                    if bid is not None and bid >= bid_min:
                        # Check DTE <= 45 (erhöht von 28 für Monthly Options)
                        exp_str = r.get('expiration', '')
                        try:
                            exp_date = datetime.strptime(exp_str, '%Y%m%d')
                            dte = (exp_date - now).days
                            if dte <= 45:
                                bid_filtered.append(r)
                        except:
                            bid_filtered.append(r)  # If parsing fails, keep it
                filtered = bid_filtered
                logger.info(f"bid_min filter (>= {bid_min}): {len(filtered)} options remaining")
                
                for r in filtered:
                    r['expiration'] = expiration
                    bid = r.get('bid')
                    strike = r.get('strike')
                    if bid and strike and strike != 0:
                        r['put_profit'] = (bid / strike) * 100
                    else:
                        r['put_profit'] = 0

                all_rows.extend(filtered)

            except Exception as e:
                logger.warning(f"  {symbol} {expiration} failed: {e}")

        # ============================================================
        # BULL PUT SPREAD LOGIC (if spread_mode enabled)
        # ============================================================
        spread_rows = []
        if spread_mode and all_rows:
            logger.info(f"  {symbol} {expiration}: Searching Bull Put Spreads (width={spread_width}, max_margin=${max_margin})")
            
            # Group by expiration
            by_exp = {}
            for r in all_rows:
                exp = r.get('expiration')
                if exp not in by_exp:
                    by_exp[exp] = []
                by_exp[exp].append(r)
            
            for exp, options in by_exp.items():
                # Sort by strike
                options.sort(key=lambda x: x.get('strike', 0))
                
                # Find spread pairs: Short higher strike, Long lower strike
                for i, short_opt in enumerate(options):
                    short_strike = short_opt.get('strike')
                    short_bid = short_opt.get('bid')
                    short_delta = short_opt.get('delta')
                    
                    if short_bid is None or short_bid <= 0 or short_delta is None:
                        continue
                    
                    # Target short delta range
                    if not (delta_min <= short_delta <= delta_max):
                        continue
                    
                    # Find long leg: lower strike by spread_width
                    target_long_strike = short_strike - spread_width
                    long_opt = None
                    for opt in options:
                        if abs(opt.get('strike', 0) - target_long_strike) < 0.01:
                            long_opt = opt
                            break
                    
                    if long_opt is None:
                        continue
                    
                    long_ask = long_opt.get('ask')
                    long_bid = long_opt.get('bid')
                    long_delta = long_opt.get('delta')
                    
                    if long_ask is None or long_ask <= 0:
                        continue
                    
                    # Calculate spread metrics
                    credit = short_bid - long_ask  # Net credit received
                    if credit <= 0:
                        continue  # Not a valid credit spread
                    
                    if credit < min_credit:
                        continue  # Credit below minimum target
                    
                    spread_width_actual = short_strike - long_opt.get('strike', 0)
                    max_loss = spread_width_actual * 100 - credit * 100  # Max loss per spread
                    margin_req = max_loss  # Margin for credit spread = max loss
                    
                    if margin_req > max_margin:
                        continue  # Exceeds max margin
                    
                    # Return on margin
                    rom = (credit * 100) / margin_req * 100 if margin_req > 0 else 0
                    
                    # Probability of profit (approx: 1 - abs(short_delta))
                    pop = (1 - abs(short_delta)) * 100 if short_delta else 0
                    
                    spread_row = {
                        'symbol': short_opt.get('symbol'),
                        'expiration': exp,
                        'short_strike': short_strike,
                        'long_strike': long_opt.get('strike'),
                        'spread_width': spread_width_actual,
                        'short_bid': short_bid,
                        'short_ask': short_opt.get('ask'),
                        'long_bid': long_bid,
                        'long_ask': long_ask,
                        'credit': credit,
                        'max_loss': max_loss,
                        'margin_req': margin_req,
                        'rom': rom,  # Return on Margin %
                        'pop': pop,  # Probability of Profit %
                        'short_delta': short_delta,
                        'long_delta': long_delta,
                        'short_volume': short_opt.get('volume'),
                        'long_volume': long_opt.get('volume'),
                        'short_iv': short_opt.get('impliedVol'),
                        'long_iv': long_opt.get('impliedVol'),
                    }
                    spread_rows.append(spread_row)
                    logger.info(f"    Spread found: {short_strike}/{long_opt.get('strike')}P credit=${credit:.2f} margin=${margin_req:.0f} ROM={rom:.1f}% POP={pop:.1f}%")

        # Combine regular options and spreads for output
        if spread_mode:
            # Sort spreads by ROM (Return on Margin)
            spread_rows.sort(key=lambda x: x.get('rom', 0), reverse=True)
            top5_spreads = spread_rows[:5]
            # For spread mode, return spreads instead of single options
            result = {
                'symbol': symbol,
                'stock_price': stock_data['last'] or stock_data['close'],
                'currency': currency,
                'expirations_processed': len(effective_month_indices),
                'total_options': len(all_rows),
                'total_spreads': len(spread_rows),
                'top5': top5_spreads,
                'is_spread': True
            }
        else:
            # Regular single options mode
            all_rows.sort(key=lambda x: x.get('put_profit', 0), reverse=True)
            top5 = all_rows[:5]
            
            result = {
                'symbol': symbol,
                'stock_price': stock_data['last'] or stock_data['close'],
                'currency': currency,
                'expirations_processed': len(effective_month_indices),
                'total_options': len(all_rows),
                'top5': top5,
                'is_spread': False
            }

        if keep_connected:
            result['ib'] = ib
            ib = None  # Don't disconnect in finally

        return result
        
    except Exception as e:
        handle_ibkr_error(f"process_symbol({symbol})", e, logger)
        return {'symbol': symbol, 'error': str(e)}
    finally:
        if ib and ib.isConnected():
            try:
                ib.disconnect()
            except Exception as e:
                logger.warning(f"process_symbol({symbol}): disconnect error - {e}")


def write_master_html(results, filepath):
    """Erzeugt HTML-Report parallel zur CSV"""
    html_path = filepath.replace('.csv', '.html')

    # Daten für HTML aufbereiten
    rows = []
    for res in results:
        if 'error' in res:
            continue
        for r in res['top5']:
            row = format_row_for_output(r)
            rows.append(row)

    if not rows:
        return

    html = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <title>Option Scan Results</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 20px; background: #fafafa; }}
        h1 {{ color: #333; }}
        .meta {{ color: #666; margin-bottom: 20px; }}
        table {{ border-collapse: collapse; width: 100%; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        th, td {{ padding: 10px 12px; text-align: right; border-bottom: 1px solid #eee; }}
        th {{ background: #f5f5f5; font-weight: 600; text-align: right; }}
        td.symbol {{ text-align: left; font-weight: 500; }}
        td.expiration {{ text-align: center; }}
        tr:hover {{ background: #fafafa; }}
        .profit {{ font-weight: 600; color: #2e7d32; }}
        .delta {{ color: #1565c0; }}
        .symbol-group {{ background: #f9f9f9; }}
    </style>
</head>
<body>
    <h1>Multi-Symbol Option Scan</h1>
    <p class="meta">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    <table>
        <thead>
            <tr>
                {''.join(f'<th>{h}</th>' for h in HEADERS)}
            </tr>
        </thead>
        <tbody>
"""
    current_symbol = None
    for row in rows:
        sym = row.get('symbol', '')
        if sym != current_symbol:
            current_symbol = sym
            html += f'        <tr class="symbol-group"><td colspan="{len(HEADERS)}"><strong>{sym}</strong></td></tr>\n'
        html += '        <tr>\n'
        for h, val in row.items():
            cls = ''
            if h == 'symbol':
                cls = 'symbol'
            elif h == 'expiration':
                cls = 'expiration'
            elif h == 'put_profit':
                cls = 'profit'
            elif h == 'delta':
                cls = 'delta'
            html += f'            <td class="{cls}">{val}</td>\n'
        html += '        </tr>\n'

    html += """        </tbody>
    </table>
</body>
</html>"""

    try:
        with open(html_path, 'w') as f:
            f.write(html)
        logger.info(f"HTML Report: {html_path}")
        return True
    except PermissionError:
        logger.error(f"write_master_html: Permission denied - {html_path}")
    except OSError as e:
        if e.errno == errno.ENOSPC:
            logger.error(f"write_master_html: Disk full - {html_path}")
        else:
            logger.error(f"write_master_html: OS error - {e}")
    except Exception as e:
        handle_file_error("write_master_html", e, logger)
    return False


def main():
    default_symbols = ['CROX', 'AAPL', 'TREX', 'TSLA', 'NVDA']
    default_month_indices = [0, 1, 2]
    
    # Argument parsing
    manual_mode = False
    delta_min = -0.50
    delta_max = -0.10
    num_chains = 0  # 0 = auto (use month_indices or default)
    order_type = 'LMT'  # LMT or REL
    output_dir = None  # Custom output directory
    bid_min = 0.80  # Minimum bid price
    bracket_mode = False  # Bracket Order (Take Profit 70% / Stop Loss 200%)
    verbose = False  # Verbose logging
    spread_mode = False  # Bull Put Spread Mode
    spread_width = 5  # Strike width for spread (default 5 points)
    max_margin = 1000  # Max margin per spread in USD
    min_credit = 0.80  # Minimum credit target for spreads in USD
    if len(sys.argv) > 1:
        args = sys.argv[1:]
        symbols = []
        month_indices = []
        exchange = 'SMART'
        currency = 'USD'
        
        # Filter out -m/--manual flag
        filtered_args = []
        i = 0
        while i < len(args):
            arg = args[i]
            if arg in ['-m', '--manual']:
                manual_mode = True
            elif arg in ['-v', '--verbose']:
                verbose = True
            elif arg in ['--delta-min', '--dmin'] and i + 1 < len(args):
                delta_min = float(args[i + 1])
                i += 1
            elif arg in ['--delta-max', '--dmax'] and i + 1 < len(args):
                delta_max = float(args[i + 1])
                i += 1
            elif arg in ['--chains', '--num-chains'] and i + 1 < len(args):
                num_chains = int(args[i + 1])
                i += 1
            elif arg in ['--order-type', '--ot'] and i + 1 < len(args):
                order_type = args[i + 1].upper()
                if order_type not in ['LMT', 'REL']:
                    print(f"Error: --order-type must be LMT or REL, got {order_type}")
                    sys.exit(1)
                i += 1
            elif arg in ['--bid-min', '--bmin'] and i + 1 < len(args):
                bid_min = float(args[i + 1])
                i += 1
            elif arg in ['--bracket', '-b']:
                bracket_mode = True
            elif arg in ['--spread', '--bull-put-spread']:
                spread_mode = True
            elif arg in ['--spread-width', '--sw'] and i + 1 < len(args):
                spread_width = int(args[i + 1])
                i += 1
            elif arg in ['--max-margin', '--mm'] and i + 1 < len(args):
                max_margin = float(args[i + 1])
                i += 1
            elif arg in ['--min-credit', '--mc'] and i + 1 < len(args):
                min_credit = float(args[i + 1])
                i += 1
            elif arg in ['--output-dir', '-o'] and i + 1 < len(args):
                output_dir = args[i + 1]
                i += 1
            else:
                filtered_args.append(arg)
            i += 1
        
        args = filtered_args
        
        for arg in args:
            if arg.isdigit():
                month_indices.append(int(arg))
            elif arg in ['SMART', 'XETRA', 'AEB', 'NYSE', 'NASDAQ', 'FWB', 'IBIS']:
                exchange = arg
            elif arg in ['USD', 'EUR']:
                currency = arg
            else:
                symbols.append(arg.upper())
        
        if not symbols:
            symbols = default_symbols
        if not month_indices and num_chains == 0:
            month_indices = default_month_indices
    else:
        symbols = default_symbols
        month_indices = default_month_indices
        exchange = 'SMART'
        currency = 'USD'
    
    # Configure logging based on verbose flag
    log_level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s : %(message)s')
    
    # Aggressively suppress ALL ib_insync loggers when not verbose
    if not verbose:
        for logger_name in ['ib_insync', 'ib_insync.ib', 'ib_insync.client', 'ib_insync.wrapper', 
                           'ib_insync.contract', 'ib_insync.order', 'ib_insync.ticker', 'ib_insync.util']:
            lg = logging.getLogger(logger_name)
            lg.setLevel(logging.CRITICAL)  # Only CRITICAL through
            lg.propagate = False  # Don't propagate to root logger
    
    # Set script's own logger level
    logger.setLevel(log_level)
    
    # Configurable output path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if output_dir:
        output_path = Path(output_dir)
    else:
        output_path = Path.cwd() / "output"
    output_path.mkdir(parents=True, exist_ok=True)
    csv_file = output_path / f"multi_symbol_top5_{timestamp}.csv"
    
    logger.info(f"Multi-Symbol Batch: {len(symbols)} Symbole, {len(month_indices)} Expirations each")
    logger.info(f"Symbols: {symbols}")
    logger.info(f"Month indices: {month_indices}")
    logger.info(f"Output: {csv_file}")
    
    results = []
    # Use unique client IDs based on PID to avoid conflicts
    base_client_id = (os.getpid() % 1000) * 1000
    for i, symbol in enumerate(symbols):
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {symbol} ({i+1}/{len(symbols)})...")
        
        defaults = SYMBOL_DEFAULTS.get(symbol, {'exchange': exchange, 'currency': currency})
        # Respect explicitly passed exchange/currency, only use defaults if not overridden
        # Since default param is 'SMART'/'USD', we check if user provided non-default via sys.argv
        # Simpler: always use passed exchange/currency (user intent), defaults only for fallback
        ex = exchange
        cur = currency
        
        # Don't keep connection for orders - create dedicated one later
        result = process_symbol(symbol, month_indices, ex, cur, base_client_id + i,
                               keep_connected=False, delta_min=delta_min, delta_max=delta_max,
                               num_chains=num_chains, order_type=order_type, bid_min=bid_min,
                               bracket_mode=bracket_mode, spread_mode=spread_mode,
                               spread_width=spread_width, max_margin=max_margin, min_credit=min_credit,
                               verbose=verbose)
        results.append(result)
        
        if 'error' in result:
            logger.error(f"{symbol}: {result['error']}")
        else:
            logger.info(f"{symbol}: {len(result['top5'])} top options, stock={result['stock_price']} {result['currency']}")
    
    # Master CSV
    write_master_csv(results, str(csv_file))
    write_master_html(results, str(csv_file))
    logger.info(f"Master CSV: {csv_file}")
    
    # Summary
    print(f"\n{'='*80}")
    print(f"MULTI-SYMBOL BATCH RESULTS - Top 5 per Symbol by Put Profit")
    print(f"{'='*80}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"File: {csv_file}")
    print(f"{'='*80}")
    
    for res in results:
        if 'error' in res:
            print(f"\n❌ {res['symbol']}: {res['error']}")
            continue
        
        is_spread = res.get('is_spread', False)
        
        if is_spread:
            print(f"\n📈 {res['symbol']} @ {res['stock_price']} {res['currency']} ({res['expirations_processed']} expirations) — BULL PUT SPREADS")
            print(f"{'Short':>7} | {'Long':>6} | {'W':>3} | {'Exp':>10} | {'Credit':>7} | {'Margin':>7} | {'ROM%':>6} | {'POP%':>5} | {'Short Δ':>7}")
            print("-" * 85)
            for r in res['top5']:
                exp_short = r['expiration'][4:] if len(r['expiration']) == 8 else r['expiration']
                print(f"{r['short_strike']:>7.1f} | {r['long_strike']:>6.1f} | {r['spread_width']:>3.0f} | {exp_short:>10} | ${r['credit']:>6.2f} | ${r['margin_req']:>6.0f} | {r['rom']:>5.1f}% | {r['pop']:>4.1f}% | {r['short_delta']:>7.4f}")
        else:
            print(f"\n📈 {res['symbol']} @ {res['stock_price']} {res['currency']} ({res['expirations_processed']} expirations)")
            print(f"{'Strike':>8} | {'Exp':>10} | {'Delta':>7} | {'Bid':>6} | {'Ask':>6} | {'Vol':>5} | {'IV':>7} | {'Profit%':>8}")
            print("-" * 85)
            for r in res['top5']:
                exp_short = r['expiration'][4:] if len(r['expiration']) == 8 else r['expiration']
                print(f"{r['strike']:>8.1f} | {exp_short:>10} | {r['delta']:>7.4f} | {r['bid']:>6.2f} | {r['ask']:>6.2f} | {r['volume']:>5.0f} | {r['impliedVol']:>6.2%} | {r['put_profit']:>7.2f}%")
    
    # Collect all top options for auto-order
    all_top_options = []
    for res in results:
        if 'error' in res:
            continue
        for r in res['top5']:
            all_top_options.append(r)
    
    if not all_top_options:
        print("\n⚠️ No options found to trade.")
        print(f"\n✅ Fertig!")
        return
    
    # Create dedicated IB connection for orders
    order_ib = connect_ib(base_client_id + 999)  # Use high client ID for orders
    if not order_ib or not order_ib.isConnected():
        print("⚠️ Could not establish order connection to IBKR.")
        print(f"\n✅ Fertig!")
        return
    
    try:
        if manual_mode:
            # MANUAL MODE: User selects which option(s) to trade
            print(f"\n{'='*80}")
            is_spread_mode = spread_mode
            if is_spread_mode:
                print(f"MANUAL MODE: Wählen Sie Bull Put Spread(s) zum Handeln")
            else:
                print(f"MANUAL MODE: Wählen Sie Option(en) zum Handeln")
            print(f"{'='*80}")
        
            # Show all options with numbers
            print(f"\nVerfügbare {'Spreads' if is_spread_mode else 'Optionen'} (Top 5 pro Symbol):")
            for idx, r in enumerate(all_top_options):
                exp_short = r['expiration'][4:] if len(r['expiration']) == 8 else r['expiration']
                # Calculate DTE (Days to Expiration)
                try:
                    exp_date = datetime.strptime(r['expiration'], '%Y%m%d')
                    dte = (exp_date - datetime.now()).days
                except Exception as e:
                    logger.warning(f"main: DTE calculation failed - {type(e).__name__}: {e}")
                    dte = '?'
                if is_spread_mode:
                    print(f"  [{idx+1}] {r['symbol']} {r['short_strike']:.0f}/{r['long_strike']:.0f}P {exp_short} | DTE={dte}d | Credit=${r['credit']:.2f} | Margin=${r['margin_req']:.0f} | ROM={r['rom']:.1f}% | POP={r['pop']:.1f}% | Short Δ={r['short_delta']:.4f}")
                else:
                    print(f"  [{idx+1}] {r['symbol']} {r['strike']:.0f}P {exp_short} | DTE={dte}d | Delta={r['delta']:.4f} | Bid=${r['bid']:.2f} | Ask=${r['ask']:.2f} | Vol={r['volume']:.0f} | IV={r['impliedVol']:.1%} | Profit={r['put_profit']:.2f}%")
        
            print(f"\nEingabe: Nummern (z.B. '1', '1,3', '1 3') oder 'q' zum Beenden")
        
            while True:
                try:
                    choice = input("\nOption(en) wählen (l=list, q=quit): ").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print("\nAbbruch.")
                    break
                if choice in ['q', 'quit', 'exit']:
                    break
                elif choice in ['l', 'list']:
                    # Re-list available options
                    print(f"\nVerfügbare {'Spreads' if is_spread_mode else 'Optionen'} (Top 5 pro Symbol):")
                    for idx, r in enumerate(all_top_options):
                        exp_short = r['expiration'][4:] if len(r['expiration']) == 8 else r['expiration']
                        try:
                            exp_date = datetime.strptime(r['expiration'], '%Y%m%d')
                            dte = (exp_date - datetime.now()).days
                        except Exception as e:
                            logger.warning(f"main: DTE calculation failed - {type(e).__name__}: {e}")
                            dte = '?'
                        if is_spread_mode:
                            print(f"  [{idx+1}] {r['symbol']} {r['short_strike']:.0f}/{r['long_strike']:.0f}P {exp_short} | DTE={dte}d | Credit=${r['credit']:.2f} | Margin=${r['margin_req']:.0f} | ROM={r['rom']:.1f}% | POP={r['pop']:.1f}% | Short Δ={r['short_delta']:.4f}")
                        else:
                            print(f"  [{idx+1}] {r['symbol']} {r['strike']:.0f}P {exp_short} | DTE={dte}d | Delta={r['delta']:.4f} | Bid=${r['bid']:.2f} | Ask=${r['ask']:.2f} | Vol={r['volume']:.0f} | IV={r['impliedVol']:.1%} | Profit={r['put_profit']:.2f}%")
                    print(f"\nEingabe: Nummern (z.B. '1', '1,3', '1 3') oder 'q' zum Beenden")
                    continue
                
                parts = choice.replace(',', ' ').split()
                try:
                    indices = [int(p) - 1 for p in parts]
                    valid = [i for i in indices if 0 <= i < len(all_top_options)]
                    invalid = [i for i in indices if i < 0 or i >= len(all_top_options)]

                    if invalid:
                        print(f"Ungültige Nummern: {[i+1 for i in invalid]}. Gültig: 1-{len(all_top_options)}")

                    for idx in valid:
                        selected = all_top_options[idx]
                        # Place order for selected option/spread
                        try:
                            defaults = SYMBOL_DEFAULTS.get(selected['symbol'].upper(), {'exchange': 'SMART', 'currency': 'USD'})
                            ex = defaults['exchange']
                            cur = defaults['currency']
                            
                            if is_spread_mode:
                                # BULL PUT SPREAD: Create both legs
                                trading_class = selected['symbol']
                                
                                # Short leg (higher strike)
                                short_opt = create_put_option(selected['symbol'], selected['expiration'],
                                                             selected['short_strike'], selected.get('multiplier', 100),
                                                             ex, trading_class)
                                # Long leg (lower strike)
                                long_opt = create_put_option(selected['symbol'], selected['expiration'],
                                                            selected['long_strike'], selected.get('multiplier', 100),
                                                            ex, trading_class)
                                order_ib.qualifyContracts(short_opt, long_opt)
                                
                                print(f"\n>>> Platzieren BULL PUT SPREAD:")
                                print(f"   Short: SELL 1 {short_opt.localSymbol}")
                                print(f"   Long:  BUY  1 {long_opt.localSymbol}")
                                print(f"   Credit Target: ${selected['credit']:.2f} | Margin: ${selected['margin_req']:.0f}")
                                
                                placed = place_spread_order(order_ib, short_opt, long_opt,
                                                          quantity=1, order_type=order_type,
                                                          credit_limit_price=selected['credit'],
                                                          auto_transmit=False, verbose=verbose)
                                if placed:
                                    print(f"✅ {selected['symbol']} BULL PUT SPREAD erfolgreich platziert!")
                                else:
                                    print(f"❌ {selected['symbol']} Spread Order nicht platziert.")
                            else:
                                # Regular single option
                                trading_class = selected['symbol']  # fallback
                                
                                opt = create_put_option(selected['symbol'], selected['expiration'],
                                                       selected['strike'], selected['multiplier'],
                                                       ex, trading_class)
                                order_ib.qualifyContracts(opt)
                                
                                mark_price = get_mark_price(order_ib, opt)
                                
                                if order_type == 'LMT':
                                    limit_price = round(mark_price - 0.05, 2) if mark_price and mark_price > 0 else selected['bid']
                                    print(f"\n>>> Platzieren SELL 1 {opt.localSymbol} @ ${limit_price:.2f} LMT (Mark: ${mark_price:.2f} - $0.05)")
                                else:
                                    limit_price = None
                                    mid_price = mark_price if mark_price and mark_price > 0 else selected['bid']
                                    print(f"\n>>> Platzieren SELL 1 {opt.localSymbol} @ REL (Midpoint, Mark: ${mid_price:.2f})")
                                placed = place_option_order(order_ib, opt, action='SELL', quantity=1,
                                                           order_type=order_type, limit_price=limit_price,
                                                           auto_transmit=False, bracket=bracket_mode)
                                if placed:
                                    print(f"✅ {selected['symbol']} PUT Order erfolgreich platziert!")
                                else:
                                    print(f"❌ {selected['symbol']} Order nicht platziert.")
                        except Exception as e:
                            handle_ibkr_error(f"main(manual:{selected['symbol']})", e, logger)
                            print(f"❌ {selected['symbol']} Fehler: {e}")

                    if valid:
                        # Refresh positions
                        pass

                except ValueError:
                    print("Eingabe: Nummern (z.B. '1', '1,3', '1 3') oder 'q'")
                except Exception as e:
                    handle_ibkr_error("main(manual loop)", e, logger)
        else:
            # AUTO MODE: Best PUT per Symbol or Best SPREAD per Symbol
            print(f"\n{'='*80}")
            if spread_mode:
                print(f"AUTO-ORDER: Best BULL PUT SPREAD per Symbol")
            else:
                print(f"AUTO-ORDER: Best PUT Options per Symbol")
            print(f"{'='*80}")
        
            # Find best option/spread per symbol
            for symbol in symbols:
                symbol_options = [r for r in all_top_options if r['symbol'] == symbol]
                if not symbol_options:
                    continue
            
                if spread_mode:
                    best = max(symbol_options, key=lambda x: x.get('rom', 0))
                    print(f"\nBester {symbol} Spread: {best['symbol']} {best['short_strike']:.0f}/{best['long_strike']:.0f}P {best['expiration'][4:]}")
                    print(f"Credit=${best['credit']:.2f} | Margin=${best['margin_req']:.0f} | ROM={best['rom']:.1f}% | POP={best['pop']:.1f}% | Short Δ={best['short_delta']:.4f}")
                else:
                    best = max(symbol_options, key=lambda x: x['put_profit'])
                    print(f"\nBeste {symbol} Option: {best['symbol']} {best['strike']:.0f}P {best['expiration'][4:]}")
                    print(f"Delta={best['delta']:.4f} | Bid=${best['bid']:.2f} | Put_Profit={best['put_profit']:.2f}%")
            
                if order_ib and order_ib.isConnected():
                    try:
                        defaults = SYMBOL_DEFAULTS.get(best['symbol'].upper(), {'exchange': 'SMART', 'currency': 'USD'})
                        ex = defaults['exchange']
                        cur = defaults['currency']
                        
                        if spread_mode:
                            # BULL PUT SPREAD
                            trading_class = best['symbol']
                            short_opt = create_put_option(best['symbol'], best['expiration'],
                                                         best['short_strike'], best.get('multiplier', 100),
                                                         ex, trading_class)
                            long_opt = create_put_option(best['symbol'], best['expiration'],
                                                        best['long_strike'], best.get('multiplier', 100),
                                                        ex, trading_class)
                            try:
                                order_ib.qualifyContracts(short_opt, long_opt)
                            except RequestError as e:
                                logger.error(f"main(auto:{symbol}): qualifyContracts failed - {e}")
                                print(f"❌ {symbol} qualifyContracts fehlgeschlagen: {e}")
                                continue
                            
                            print(f"\n>>> Auto-Platzieren BULL PUT SPREAD:")
                            print(f"   Short: SELL 1 {short_opt.localSymbol}")
                            print(f"   Long:  BUY  1 {long_opt.localSymbol}")
                            print(f"   Credit Target: ${best['credit']:.2f} | Margin: ${best['margin_req']:.0f}")
                            
                            placed = place_spread_order(order_ib, short_opt, long_opt,
                                                      quantity=1, order_type=order_type,
                                                      credit_limit_price=best['credit'],
                                                      auto_transmit=False, verbose=verbose)
                            if placed:
                                print(f"✅ {symbol} BULL PUT SPREAD erfolgreich platziert!")
                            else:
                                print(f"❌ {symbol} Spread Order nicht platziert.")
                        else:
                            # Regular single option
                            trading_class = best['symbol']
                            opt = create_put_option(best['symbol'], best['expiration'],
                                                   best['strike'], best['multiplier'],
                                                   ex, trading_class)
                            try:
                                order_ib.qualifyContracts(opt)
                            except RequestError as e:
                                logger.error(f"main(auto:{symbol}): qualifyContracts failed - {e}")
                                print(f"❌ {symbol} qualifyContracts fehlgeschlagen: {e}")
                                continue
                            
                            mark_price = get_mark_price(order_ib, opt)
                            
                            if order_type == 'LMT':
                                limit_price = round(mark_price - 0.05, 2) if mark_price and mark_price > 0 else best['bid']
                                print(f">>> Platzieren SELL 1 {opt.localSymbol} @ ${limit_price:.2f} LMT (Mark: ${mark_price:.2f} - $0.05)")
                            else:
                                limit_price = None
                                mid_price = mark_price if mark_price and mark_price > 0 else best['bid']
                                print(f">>> Platzieren SELL 1 {opt.localSymbol} @ REL (Midpoint, Mark: ${mid_price:.2f})")
                            placed = place_option_order(order_ib, opt, action='SELL', quantity=1,
                                                       order_type=order_type, limit_price=limit_price,
                                                       auto_transmit=False, bracket=bracket_mode)
                            if placed:
                                print(f"✅ {symbol} PUT Order erfolgreich platziert!")
                            else:
                                print(f"❌ {symbol} Order nicht platziert.")
                    except Exception as e:
                        handle_ibkr_error(f"main(auto:{symbol})", e, logger)
                        print(f"Fehler: {e}")
                else:
                    print("⚠️ Keine aktive Connection für Auto-Order.")
                    break
    
    finally:
        # Clean up order connection
        if order_ib and order_ib.isConnected():
            try:
                order_ib.disconnect()
            except Exception as e:
                logger.warning(f"main: order_ib disconnect error - {e}")
    
    print(f"\n✅ Fertig!")

if __name__ == '__main__':
    main()