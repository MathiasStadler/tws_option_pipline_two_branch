#!/usr/bin/env python3
# Close Position Script: Listet offene Positionen + offene Orders, schließt per Auswahl
# Usage: python3 close_position.py [-h|--help]

import sys
from ib_insync import IB, Option, Order, Stock, Trade

HELP_TEXT = """
Close Position Script - IBKR TWS Position Manager
==================================================

Usage: python3 close_position.py [-h|--help]

Beschreibung:
  Zeigt alle offenen Positionen und offenen Orders an und ermöglicht
  das interaktive Schließen von Positionen oder Abbrechen von Orders.

Aktionen im interaktiven Menü:
  [1-N]           Einzelne Position schließen (Market Order Gegenseite)
  [1,3 oder 1 3]  Mehrere Positionen gleichzeitig schließen
  [c + Order#]    Offene Order abbrechen (z.B. 'c1' für Order #1)
  [r]             Liste aktualisieren (Refresh)
  [q]             Beenden

Beispiele:
  python3 close_position.py          # Startet interaktiven Modus
  python3 close_position.py -h       # Zeigt diese Hilfe
  python3 close_position.py --help   # Zeigt diese Hilfe

Positionen:
  - Short Positionen (negativ) -> BUY Order zum Schließen
  - Long Positionen (positiv)  -> SELL Order zum Schließen
  - Market Order für sofortige Ausführung
  - y/n Bestätigung vor Ausführung

Orders:
  - Zeigt alle offenen Orders (PendingSubmit, PreSubmitted, Submitted)
  - Abbrechen mit 'c' + Order-Nummer (z.B. 'c1')

Voraussetzungen:
  - SSH Tunnel zu TWS: ssh -L 7496:localhost:7496 user@host
  - TWS API aktiviert (Port 7496)
  - ib_insync installiert

Datei: /home/hermes/tws_option_pipeline/src/close_position.py
"""

def print_help():
    print(HELP_TEXT)
    sys.exit(0)

def connect_ib(client_id=200):
    ib = IB()
    ib.connect('127.0.0.1', 7496, clientId=client_id, timeout=10, readonly=False)
    return ib

def get_positions(ib):
    """Holt alle offenen Positionen"""
    positions = ib.positions()
    return [p for p in positions if p.position != 0]

def get_open_orders(ib):
    """Holt alle offenen Orders"""
    trades = ib.trades()
    open_trades = []
    for t in trades:
        status = t.orderStatus.status
        if status in ('PendingSubmit', 'PreSubmitted', 'Submitted'):
            open_trades.append(t)
    return open_trades

def get_pending_closes(ib):
    """Mappt conId -> Liste von pending close orders (qty, action, status)"""
    pending = {}
    for t in get_open_orders(ib):
        c = t.contract
        conid = c.conId
        action = t.order.action
        qty = t.order.totalQuantity
        status = t.orderStatus.status
        if conid not in pending:
            pending[conid] = []
        pending[conid].append({'action': action, 'qty': qty, 'status': status, 'orderId': t.order.orderId})
    return pending

def display_positions_with_orders(positions, pending_closes):
    """Zeigt Positionen + markiert pending close orders"""
    if not positions:
        print("Keine offenen Positionen.")
        return
    
    print(f"\n{'='*110}")
    print(f"OFFENE POSITIONEN ({len(positions)})  |  PENDING CLOSE ORDERS: {sum(len(v) for v in pending_closes.values())}")
    print(f"{'='*110}")
    print(f"{'#':>3} | {'Symbol':<10} | {'Type':<5} | {'Exp':<10} | {'Strike':>7} | {'R':<2} | {'Pos':>6} | {'AvgCost':>9} | {'ConId':>10} | {'PENDING CLOSE':<25}")
    print("-" * 110)
    
    for i, p in enumerate(positions, 1):
        c = p.contract
        sym = c.symbol
        typ = c.secType
        exp = c.lastTradeDateOrContractMonth if hasattr(c, 'lastTradeDateOrContractMonth') and c.lastTradeDateOrContractMonth else ''
        strike = c.strike if hasattr(c, 'strike') and c.strike else 0
        right = c.right if hasattr(c, 'right') and c.right else ''
        pos = p.position
        avg = p.avgCost
        conid = c.conId
        
        # Pending close info
        pending_info = ""
        if conid in pending_closes:
            orders = pending_closes[conid]
            parts = []
            for o in orders:
                parts.append(f"{o['action']} {o['qty']} [{o['status']}]")
            pending_info = "; ".join(parts)
            pending_mark = f"🔄 {pending_info}"
        else:
            pending_mark = ""
        
        print(f"{i:>3} | {sym:<10} | {typ:<5} | {exp:<10} | {strike:>7.2f} | {right:<2} | {pos:>6} | {avg:>9.4f} | {conid:>10} | {pending_mark}")

def display_open_orders(ib):
    """Zeigt alle offenen Orders separat"""
    trades = get_open_orders(ib)
    if not trades:
        return
    print(f"\n{'='*80}")
    print(f"ALLE OFFENEN ORDERS ({len(trades)})")
    print(f"{'='*80}")
    print(f"{'#':>3} | {'Symbol':<10} | {'Exp':<10} | {'Str':>7} | {'R':<2} | {'Act':<5} | {'Qty':>5} | {'Type':<5} | {'Status':<15} | {'OrderId':>8}")
    print("-" * 80)
    for i, t in enumerate(trades, 1):
        c = t.contract
        sym = c.symbol
        exp = c.lastTradeDateOrContractMonth if hasattr(c, 'lastTradeDateOrContractMonth') else ''
        strike = c.strike if hasattr(c, 'strike') and c.strike else 0
        right = c.right if hasattr(c, 'right') else ''
        action = t.order.action
        qty = t.order.totalQuantity
        otype = t.order.orderType
        status = t.orderStatus.status
        oid = t.order.orderId
        print(f"{i:>3} | {sym:<10} | {exp:<10} | {strike:>7.2f} | {right:<2} | {action:<5} | {qty:>5} | {otype:<5} | {status:<15} | {oid:>8}")

def close_position(ib, position):
    """Schließt eine Position (Market Order Gegenseite)"""
    c = position.contract
    pos = position.position
    
    # Gegenseite bestimmen
    action = 'SELL' if pos > 0 else 'BUY'
    qty = abs(pos)
    
    # Exchange setzen (wichtig für Order-Validierung)
    if not c.exchange or c.exchange == '':
        c.exchange = 'SMART'
    
    # Order erstellen
    order = Order()
    order.action = action
    order.orderType = 'MKT'
    order.totalQuantity = qty
    order.transmit = True
    order.tif = 'DAY'  # Explizit DAY setzen
    
    print(f"\n{'='*60}")
    print(f"POSITION SCHLIESSEN")
    print(f"{'='*60}")
    print(f"Contract: {c.localSymbol}")
    print(f"Action:   {action} {qty} (Current: {pos})")
    print(f"OrderType: MKT")
    print(f"{'='*60}")
    
    # Bestätigung
    confirm = input("Position schließen? (y/n): ").strip().lower()
    if confirm not in ['y', 'yes', 'j', 'ja']:
        print("Abgebrochen.")
        return False
    
    trade = ib.placeOrder(c, order)
    ib.sleep(2)
    
    print(f"Status: {trade.orderStatus.status}")
    if trade.fills:
        for f in trade.fills:
            print(f"Fill: {f.execution.shares} @ {f.execution.price} @ {f.execution.time}")
    return True

def cancel_order(ib, trade):
    """Bricht eine offene Order ab"""
    print(f"Abbrechen Order {trade.order.orderId} ({trade.contract.localSymbol})...")
    ib.cancelOrder(trade.order)
    ib.sleep(1)
    print(f"Status: {trade.orderStatus.status}")

def main():
    # Help argument prüfen
    if len(sys.argv) > 1 and sys.argv[1] in ('-h', '--help'):
        print_help()
    
    ib = None
    try:
        ib = connect_ib(200)
        print("✅ Verbunden mit TWS")
        
        while True:
            # Daten holen
            positions = get_positions(ib)
            pending_closes = get_pending_closes(ib)
            
            # Anzeigen
            display_positions_with_orders(positions, pending_closes)
            display_open_orders(ib)
            
            if not positions:
                print("\nKeine offenen Positionen.")
                break
            
            # Menü
            print(f"\n{'='*60}")
            print("AKTIONEN:")
            print("  [1-N] Position schließen (Market Order)")
            print("  [1,3 oder 1 3] Mehrere Positionen schließen")
            print("  [c + Order#] Order abbrechen (z.B. 'c1')")
            print("  [r] Refresh")
            print("  [q] Beenden")
            print(f"{'='*60}")
            
            choice = input("\nAktion: ").strip().lower()
            
            if choice in ['q', 'quit', 'exit']:
                break
            elif choice == 'r':
                continue  # Refresh
            elif choice.startswith('c'):
                # Order abbrechen
                try:
                    oid = int(choice[1:]) - 1
                    trades = get_open_orders(ib)
                    if 0 <= oid < len(trades):
                        cancel_order(ib, trades[oid])
                    else:
                        print("Ungültige Order-Nummer.")
                except ValueError:
                    print("Format: c + Zahl (z.B. c1)")
            else:
                # Mehrere Positionen parsen (z.B. "1,3" oder "1 3")
                parts = choice.replace(',', ' ').split()
                try:
                    indices = [int(p) - 1 for p in parts]
                    valid = [i for i in indices if 0 <= i < len(positions)]
                    invalid = [i for i in indices if i < 0 or i >= len(positions)]
                    
                    if invalid:
                        print(f"Ungültige Nummern: {[i+1 for i in invalid]}. Gültig: 1-{len(positions)}")
                    
                    for idx in valid:
                        close_position(ib, positions[idx])
                    
                    if valid:
                        # Refresh nach allen Closes
                        positions = get_positions(ib)
                        pending_closes = get_pending_closes(ib)
                        display_positions_with_orders(positions, pending_closes)
                        display_open_orders(ib)
                        
                except ValueError:
                    print("Eingabe: Zahl(en) (Position), 'c#' (Order abbrechen), 'r' (Refresh), 'q' (Quit)")
                    
    except KeyboardInterrupt:
        print("\nAbbruch.")
    except Exception as e:
        print(f"Fehler: {e}")
    finally:
        if ib and ib.isConnected():
            ib.disconnect()
        print("\n✅ Fertig!")

if __name__ == '__main__':
    main()