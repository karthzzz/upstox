"""
Strategy Execution Engine
=========================
Modes: paper (default), sandbox (Upstox API), live (Kotak Neo - coming soon)

Architecture:
  DATA LAYER    →  Upstox MarketDataStreamerV3 (from upstox_live.py)
  STRATEGY      →  OptionFutureMomentum (from upstox_live.py)
  EXECUTION     →  OrderExecutor (this file — paper / sandbox / live)

The strategy decides WHEN to buy/sell. The executor decides HOW.
Swap executors without touching strategy code.

Usage:
  python upstox_executor.py                       # paper mode (same as upstox_live.py)
  python upstox_executor.py --mode sandbox        # Upstox sandbox orders
  python upstox_executor.py --mode live           # Kotak Neo (coming soon)

Requires: upstox_live.py in same directory (imports strategy + data layer)
"""
import json
import os
import sys
import time
import threading
from datetime import datetime, timezone, timedelta
from abc import ABC, abstractmethod
from enum import Enum

IST = timezone(timedelta(hours=5, minutes=30))

# ═══════════════════════════════════════════════════════════════
# Import data + strategy layer from upstox_live.py
# ═══════════════════════════════════════════════════════════════
try:
    from upstox_live import (
        # Config
        INDICES, MCX_COMMODITIES, CURRENCY_SEARCH, SAVE_DIR,
        # SDK helpers
        make_client, fetch_ltp, validate_token, show_account_info,
        decode_jwt, search_instrument, clean_expiry,
        # Option chain
        get_expiries, get_option_chain, display_chain, pick_options_from_chain,
        # Data extraction
        extract_ltp, extract_close, extract_field, extract_greek, extract_best_bid_ask,
        # File writer
        InstrumentWriter,
        # Strategy classes
        Strategy, MomentumTrailingStop, OptionFutureMomentum,
        STRATEGIES,
        # Menu helpers
        pick_index, add_index_options, add_index_for_streaming,
        add_futures, add_stock, add_custom_key,
        add_mcx_commodity, add_currency,
        # Strategy setup helpers
        _find_future, _find_counterpart_option,
        # Logging
        TeeLogger, start_logging, stop_logging,
    )
    print("  ✓ Imported strategy + data layer from upstox_live.py")
except ImportError as e:
    print(f"  ✗ Could not import upstox_live.py: {e}")
    print(f"  Make sure upstox_live.py is in the same directory.")
    sys.exit(1)

import upstox_client
from upstox_client.rest import ApiException


# ═══════════════════════════════════════════════════════════════
# EXECUTION MODE
# ═══════════════════════════════════════════════════════════════
class ExecMode(Enum):
    PAPER = "paper"         # In-memory simulation (no API calls)
    SANDBOX = "sandbox"     # Upstox sandbox (real API, fake money)
    LIVE = "live"           # Real broker orders (Kotak Neo)


# ═══════════════════════════════════════════════════════════════
# ORDER EXECUTOR — Abstract Base
# ═══════════════════════════════════════════════════════════════
class OrderExecutor(ABC):
    """
    Abstract interface for order execution.
    Strategy calls buy()/sell(), executor handles the HOW.
    All executors track trades, P&L, and position state identically.
    """
    def __init__(self, instrument_name, lot_size=1, max_trades=50):
        self.instrument_name = instrument_name
        self.lot_size = lot_size
        self.max_trades = max_trades
        self.mode = ExecMode.PAPER  # override in subclass

        # Position state (same as PaperTrader)
        self.in_position = False
        self.entry_price = None
        self.entry_time = None
        self.peak_since_entry = None

        # Trade history
        self.trades = []
        self.total_pnl = 0.0

        # Current tick
        self.current_ltp = None
        self.current_time = None

        # Order tracking
        self.last_order_id = None

    def can_buy(self):
        return not self.in_position and len(self.trades) < self.max_trades

    def can_sell(self):
        return self.in_position

    def buy(self, price, timestamp):
        """Execute a buy. Returns True if successful."""
        if not self.can_buy():
            return False

        # Execute via broker (or simulate)
        order_id, fill_price = self._execute_buy(price, timestamp)

        if order_id is not None and str(order_id).startswith(("REJECTED_", "ERROR_", "BLOCKED_", "PENDING_", "UNVERIFIED_", "FAILED_")):
            print(f"  \033[91m[ORDER FAILED] BUY not executed — strategy will NOT enter position\033[0m")
            return False

        if fill_price is None:
            fill_price = price  # fallback to LTP if no fill price returned

        self.in_position = True
        self.entry_price = fill_price
        self.entry_time = timestamp
        self.peak_since_entry = fill_price
        self.last_order_id = order_id

        self._print_action("BUY", fill_price, timestamp, order_id)
        return True

    def sell(self, price, timestamp):
        """Execute a sell. Returns True if successful."""
        if not self.can_sell():
            return False

        # Execute via broker (or simulate)
        order_id, fill_price = self._execute_sell(price, timestamp)

        # CRITICAL: If broker rejected, do NOT exit position
        if order_id is not None and str(order_id).startswith(("REJECTED_", "ERROR_", "BLOCKED_", "PENDING_", "UNVERIFIED_", "FAILED_")):
            print(f"  \033[91m[ORDER FAILED] SELL not executed — position remains OPEN\033[0m")
            return False

        if fill_price is None:
            fill_price = price

        pnl = (fill_price - self.entry_price) * self.lot_size
        pnl_pct = ((fill_price - self.entry_price) / self.entry_price) * 100 if self.entry_price else 0

        trade = {
            "trade_no": len(self.trades) + 1,
            "entry_time": self.entry_time,
            "entry_price": self.entry_price,
            "exit_time": timestamp,
            "exit_price": fill_price,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "peak": self.peak_since_entry,
            "buy_order_id": self.last_order_id,
            "sell_order_id": order_id,
        }
        self.trades.append(trade)
        self.total_pnl += pnl

        self._print_action("SELL", fill_price, timestamp, order_id)
        self.print_trade_table()

        # Reset
        self.in_position = False
        self.entry_price = None
        self.entry_time = None
        self.peak_since_entry = None
        self.last_order_id = None
        return True

    def update_tick(self, ltp, timestamp):
        self.current_ltp = ltp
        self.current_time = timestamp
        if self.in_position and ltp > (self.peak_since_entry or 0):
            self.peak_since_entry = ltp

    def unrealized_pnl(self):
        if not self.in_position or self.current_ltp is None:
            return 0, 0
        pnl = (self.current_ltp - self.entry_price) * self.lot_size
        pct = ((self.current_ltp - self.entry_price) / self.entry_price) * 100
        return pnl, pct

    def status_line(self, strategy=None):
        mode_tag = f"[{self.mode.value.upper()}] "
        if self.in_position:
            ur_pnl, ur_pct = self.unrealized_pnl()
            sign = "+" if ur_pnl >= 0 else ""
            stop_info = ""
            if strategy and hasattr(strategy, 'stop_loss') and strategy.stop_loss is not None:
                bev_tag = " BEV✓" if getattr(strategy, '_breakeven_active', False) else ""
                stop_info = f" │ SL: ₹{strategy.stop_loss:.2f}{bev_tag} │ Peak: ₹{strategy.peak_price:.2f}"
            return (
                f"\033[1m[STRATEGY]\033[0m {mode_tag}LONG @ ₹{self.entry_price:.2f} │ "
                f"LTP: ₹{self.current_ltp:.2f} │ "
                f"Unrealized: {sign}₹{ur_pnl:.2f} ({sign}{ur_pct:.1f}%){stop_info} │ "
                f"Realized: ₹{self.total_pnl:.2f} │ "
                f"Trades: {len(self.trades)}/{self.max_trades}"
            )
        else:
            return (
                f"\033[1m[STRATEGY]\033[0m {mode_tag}NO POSITION │ "
                f"Realized: ₹{self.total_pnl:.2f} │ "
                f"Trades: {len(self.trades)}/{self.max_trades}"
            )

    # ── Subclass must implement these ──
    @abstractmethod
    def _execute_buy(self, price, timestamp):
        """Place a buy order. Returns (order_id, fill_price) or (None, None)."""
        pass

    @abstractmethod
    def _execute_sell(self, price, timestamp):
        """Place a sell order. Returns (order_id, fill_price) or (None, None)."""
        pass

    # ── Display helpers (same as PaperTrader) ──
    def _print_action(self, action, price, timestamp, order_id=None):
        if isinstance(timestamp, str):
            time_str = timestamp.split("T")[-1][:12] if "T" in timestamp else timestamp
        else:
            time_str = timestamp.strftime("%H:%M:%S.%f")[:-3]
        color = "\033[92m" if action == "BUY" else "\033[91m"
        reset = "\033[0m"
        mode_str = f"  [{self.mode.value.upper()}]" if self.mode != ExecMode.PAPER else ""
        oid_str = f"  │  OID: {order_id}" if order_id else ""
        print(f"\n  {color}{'═'*60}")
        print(f"  ▶ {action} @ ₹{price:.2f}  │  {time_str}  │  {self.instrument_name}{mode_str}{oid_str}")
        if action == "SELL" and self.trades:
            t = self.trades[-1]
            sign = "+" if t["pnl"] >= 0 else ""
            print(f"    P&L: {sign}₹{t['pnl']:.2f} ({sign}{t['pnl_pct']:.1f}%)")
        print(f"  {'═'*60}{reset}\n")

    def print_trade_table(self):
        if not self.trades:
            return
        print(f"\n  {'─'*95}")
        print(f"  {'#':>4} │ {'Entry Time':>12} │ {'Entry ₹':>10} │ {'Exit Time':>12} │ {'Exit ₹':>10} │ {'P&L ₹':>10} │ {'P&L %':>8} │ {'Mode'}")
        print(f"  {'─'*4} │ {'─'*12} │ {'─'*10} │ {'─'*12} │ {'─'*10} │ {'─'*10} │ {'─'*8} │ {'─'*7}")
        for t in self.trades:
            et = t["entry_time"].split("T")[-1][:8] if isinstance(t["entry_time"], str) and "T" in t["entry_time"] else str(t["entry_time"])[-8:]
            xt = t["exit_time"].split("T")[-1][:8] if isinstance(t["exit_time"], str) and "T" in t["exit_time"] else str(t["exit_time"])[-8:]
            sign = "+" if t["pnl"] >= 0 else ""
            c = "\033[92m" if t["pnl"] >= 0 else "\033[91m"
            r = "\033[0m"
            print(f"  {t['trade_no']:>4} │ {et:>12} │ {t['entry_price']:>10.2f} │ {xt:>12} │ {t['exit_price']:>10.2f} │ {c}{sign}{t['pnl']:>9.2f}{r} │ {c}{sign}{t['pnl_pct']:>7.1f}%{r} │ {self.mode.value}")
        print(f"  {'─'*95}")
        tc = "\033[92m" if self.total_pnl >= 0 else "\033[91m"
        ts = "+" if self.total_pnl >= 0 else ""
        r = "\033[0m"
        wins = sum(1 for t in self.trades if t["pnl"] > 0)
        print(f"  {'SUM':>4} │ {'':>12} │ {'':>10} │ {'':>12} │ {'':>10} │ {tc}{ts}{self.total_pnl:>9.2f}{r} │ W:{wins}/{len(self.trades)}")
        print(f"  {'─'*95}\n")


# ═══════════════════════════════════════════════════════════════
# PAPER EXECUTOR — In-memory simulation (no API calls)
# ═══════════════════════════════════════════════════════════════
class PaperExecutor(OrderExecutor):
    """Same behavior as the original PaperTrader. Zero latency, fills at LTP."""
    def __init__(self, instrument_name, lot_size=1, max_trades=50):
        super().__init__(instrument_name, lot_size, max_trades)
        self.mode = ExecMode.PAPER

    def _execute_buy(self, price, timestamp):
        return (None, price)  # instant fill at LTP

    def _execute_sell(self, price, timestamp):
        return (None, price)


# ═══════════════════════════════════════════════════════════════
# UPSTOX SANDBOX EXECUTOR — Real API calls, fake money
# ═══════════════════════════════════════════════════════════════
class UpstoxSandboxExecutor(OrderExecutor):
    """
    Places real orders via Upstox Sandbox API.
    Orders are validated like live but no real money moves.
    Fills at the requested price (sandbox doesn't match against a real book).
    """
    def __init__(self, instrument_name, instrument_token, lot_size=1, max_trades=50,
                 sandbox_token=None):
        super().__init__(instrument_name, lot_size, max_trades)
        self.mode = ExecMode.SANDBOX
        self.instrument_token = instrument_token  # e.g. "NSE_FO|61720"

        # Setup sandbox client
        config = upstox_client.Configuration(sandbox=True)
        config.access_token = sandbox_token or ""
        self._client = upstox_client.ApiClient(config)
        self._order_api = upstox_client.OrderApiV3(self._client)
        self._validated = False

    def validate(self):
        """Test the sandbox connection with a harmless API call."""
        try:
            # V3 API may use different method names — try multiple
            for method_name in ['get_order_book', 'get_order_details', 'order_book']:
                if hasattr(self._order_api, method_name):
                    getattr(self._order_api, method_name)()
                    print(f"  ✓ Sandbox connection valid (via {method_name})")
                    self._validated = True
                    return True
            # No known method found — just mark as validated and test on first order
            print(f"  ⚠ No order book method found in SDK V3. Will validate on first order.")
            self._validated = True
            return True
        except ApiException as e:
            print(f"  ⚠ Sandbox order book check: {e.status if hasattr(e, 'status') else e}")
            print(f"  Will validate on first real order.")
            self._validated = True
            return True
        except Exception as e:
            print(f"  ⚠ Sandbox validation: {e}")
            self._validated = True  # proceed anyway — real test is place_order
            return True

    def _execute_buy(self, price, timestamp):
        """Place a BUY order via Upstox sandbox."""
        try:
            body = upstox_client.PlaceOrderV3Request(
                quantity=self.lot_size,
                product="I",           # intraday
                validity="DAY",
                price=round(price, 2),
                instrument_token=self.instrument_token,
                order_type="LIMIT",      # LIMIT at LTP for more realistic fill
                transaction_type="BUY",
                disclosed_quantity=0,
                trigger_price=0,
                is_amo=False,
                slice=False,
            )
            resp = self._order_api.place_order(body)
            data = resp.data if hasattr(resp, "data") else resp
            if isinstance(data, dict):
                order_id = data.get("order_id") or data.get("orderId")
            elif hasattr(data, "order_id"):
                order_id = data.order_id
            else:
                order_id = str(data)

            print(f"  \033[96m[SANDBOX] BUY order placed → OID: {order_id}\033[0m")
            return (order_id, price)  # sandbox fills at requested price

        except ApiException as e:
            error_body = e.body if hasattr(e, 'body') else str(e)
            print(f"  \033[91m[SANDBOX] BUY order FAILED: {error_body}\033[0m")
            # Fall back to paper fill so the strategy doesn't break
            return (f"FAILED_{timestamp}", price)

        except Exception as e:
            print(f"  \033[91m[SANDBOX] BUY exception: {e}\033[0m")
            return (f"ERROR_{timestamp}", None)

    def _execute_sell(self, price, timestamp):
        """Place a SELL order via Upstox sandbox."""
        try:
            body = upstox_client.PlaceOrderV3Request(
                quantity=self.lot_size,
                product="I",
                validity="DAY",
                price=round(price, 2),
                instrument_token=self.instrument_token,
                order_type="LIMIT",
                transaction_type="SELL",
                disclosed_quantity=0,
                trigger_price=0,
                is_amo=False,
                slice=False,
            )
            resp = self._order_api.place_order(body)
            data = resp.data if hasattr(resp, "data") else resp
            if isinstance(data, dict):
                order_id = data.get("order_id") or data.get("orderId")
            elif hasattr(data, "order_id"):
                order_id = data.order_id
            else:
                order_id = str(data)

            print(f"  \033[96m[SANDBOX] SELL order placed → OID: {order_id}\033[0m")
            return (order_id, price)

        except ApiException as e:
            error_body = e.body if hasattr(e, 'body') else str(e)
            print(f"  \033[91m[SANDBOX] SELL order FAILED: {error_body}\033[0m")
            return (f"FAILED_{timestamp}", price)

        except Exception as e:
            print(f"  \033[91m[SANDBOX] SELL exception: {e}\033[0m")
            return (f"ERROR_{timestamp}", None)


# ═══════════════════════════════════════════════════════════════
# KOTAK NEO EXECUTOR — Live trading via Kotak Neo API (v3.0+)
# ═══════════════════════════════════════════════════════════════
# Install the NEW actively maintained SDK (supersedes kotak-neo-api-v2):
#   pip install kotakneoapi
# Repo: https://github.com/Kotak-Neo/kotak-neo-python

def _show_kotak_profile(client):
    """Show Kotak Neo account info after login."""
    limits = None
    # Try with kwargs first (documented), fall back to bare call (some SDK versions)
    for call_style in ["kwargs", "bare"]:
        try:
            if call_style == "kwargs":
                limits = client.limits(segment="ALL", exchange="ALL", product="ALL")
            else:
                limits = client.limits()
            break
        except TypeError:
            continue  # try next style
        except Exception as e:
            print(f"  ⚠ Could not fetch account info: {e}")
            print(f"    (This is normal for UAT/sandbox. Trading should still work.)")
            return

    if limits is None:
        print(f"  ⚠ Could not fetch account info (limits API not available)")
        return

    try:
        if isinstance(limits, dict):
            data = limits.get("data", limits)
            if isinstance(data, dict):
                cash = data.get("Net", data.get("cash", data.get("availablecash", "?")))
                margin_used = data.get("MarginUsed", data.get("marginused", data.get("MarginUsedPrsnt", "?")))
                collateral = data.get("CollateralValue", data.get("collateral", "?"))
                category = data.get("Category", "")
                print(f"\n  ┌─── Kotak Account Info ─────────────────────────┐")
                if category:
                    print(f"  │  Category      : {category}")
                print(f"  │  Available (Net): ₹{cash}")
                print(f"  │  Margin used    : ₹{margin_used}")
                print(f"  │  Collateral     : ₹{collateral}")
                print(f"  └─────────────────────────────────────────────────┘")
            elif isinstance(data, list) and len(data) > 0:
                entry = data[0] if isinstance(data[0], dict) else {}
                cash = entry.get("Net", entry.get("cash", "?"))
                margin_used = entry.get("MarginUsed", entry.get("marginused", "?"))
                print(f"\n  ┌─── Kotak Account Info ─────────────────────────┐")
                print(f"  │  Available : ₹{cash}  │  Used : ₹{margin_used}")
                print(f"  └─────────────────────────────────────────────────┘")
            else:
                print(f"  Account info: {limits}")
        else:
            print(f"  Account info: {limits}")
    except Exception as e:
        print(f"  ⚠ Error parsing account info: {e}")


def _show_kotak_positions(client):
    """Show open positions after Kotak login."""
    try:
        resp = client.positions()
        if not resp:
            print(f"\n  Positions: None (no open positions)")
            return

        # Handle dict response with data list
        if isinstance(resp, dict):
            stat = resp.get("stat", "")
            st_code = resp.get("stCode", 200)
            # "Not_Ok" with 5203 = "No Data" — means zero positions, not an error
            if stat == "Not_Ok" and st_code == 5203:
                print(f"\n  Positions: No open positions")
                return
            if stat == "Not_Ok":
                print(f"\n  Positions: {resp.get('errMsg', 'error')}")
                return
            positions = resp.get("data", [])
        elif isinstance(resp, list):
            positions = resp
        else:
            print(f"\n  Positions: {resp}")
            return

        if not positions:
            print(f"\n  Positions: No open positions")
            return

        print(f"\n  ┌─── Open Positions ({len(positions)}) ─────────────────────────┐")
        for pos in positions:
            sym = pos.get("trdSym") or pos.get("sym") or "?"
            prod = pos.get("prod") or "?"
            buy_qty = int(pos.get("flBuyQty") or 0) + int(pos.get("cfBuyQty") or 0)
            sell_qty = int(pos.get("flSellQty") or 0) + int(pos.get("cfSellQty") or 0)
            net_qty = buy_qty - sell_qty
            buy_amt = float(pos.get("buyAmt") or 0) + float(pos.get("cfBuyAmt") or 0)
            sell_amt = float(pos.get("sellAmt") or 0) + float(pos.get("cfSellAmt") or 0)
            lot_sz = int(pos.get("lotSz") or 1)
            exch = pos.get("exSeg") or ""
            expiry = pos.get("expDt") or ""

            # For F&O, show in lots
            if lot_sz > 1 and net_qty != 0:
                lots = net_qty // lot_sz
                qty_str = f"{net_qty} ({lots} lot{'s' if abs(lots) > 1 else ''})"
            else:
                qty_str = str(net_qty)

            side = "LONG" if net_qty > 0 else "SHORT" if net_qty < 0 else "FLAT"
            pnl = sell_amt - buy_amt  # booked P&L (unrealized needs LTP)
            pnl_sign = "+" if pnl >= 0 else ""

            print(f"  │  {sym:<25} {side:>5} {qty_str:>12} │ {prod} {exch}")
            if expiry:
                print(f"  │  {'':25} Exp: {expiry}  │ Booked: {pnl_sign}₹{pnl:.2f}")
        print(f"  └─────────────────────────────────────────────────┘")

    except Exception as e:
        print(f"  ⚠ Could not fetch positions: {e}")


def _show_kotak_holdings(client):
    """Show portfolio holdings after Kotak login — confirms session is alive."""
    try:
        resp = client.holdings()
        if not resp:
            print(f"\n  Holdings: None (empty portfolio)")
            return

        if isinstance(resp, dict):
            # Check for error responses
            if resp.get("Error"):
                print(f"\n  ⚠ Holdings: {resp.get('Error')}")
                return
            st_code = resp.get("stCode", 200)
            stat = resp.get("stat", "")
            if st_code == 403 or "invalid session" in str(resp).lower():
                print(f"\n  \033[91m✗ Holdings failed: SESSION INVALID — login may have failed!\033[0m")
                return
            if stat == "Not_Ok":
                # 5203 = no data — means zero holdings, not an error
                if st_code == 5203:
                    print(f"\n  Holdings: No stocks in portfolio (empty demat)")
                else:
                    print(f"\n  Holdings: {resp.get('errMsg', 'error')}")
                return
            holdings = resp.get("data", [])
        elif isinstance(resp, list):
            holdings = resp
        else:
            holdings = []

        if not holdings:
            print(f"\n  Holdings: No stocks in portfolio (empty demat)")
            print(f"  \033[92m✓ Session confirmed active (holdings API responded)\033[0m")
            return

        total_value = 0
        total_cost = 0
        print(f"\n  ┌─── Holdings ({len(holdings)}) ──────────────────────────────────┐")
        for h in holdings:
            sym = h.get("displaySymbol") or h.get("symbol") or "?"
            qty = h.get("quantity") or h.get("sellableQuantity") or 0
            avg = float(h.get("averagePrice") or 0)
            mkt = float(h.get("mktValue") or 0)
            cost = float(h.get("holdingCost") or avg * int(qty))
            pnl = mkt - cost if mkt > 0 else 0
            pnl_sign = "+" if pnl >= 0 else ""
            total_value += mkt
            total_cost += cost
            print(f"  │  {sym:<15} qty={qty:<6} avg=₹{avg:<10.2f} mkt=₹{mkt:<10.2f} {pnl_sign}₹{pnl:.2f}")
        total_pnl = total_value - total_cost
        pnl_sign = "+" if total_pnl >= 0 else ""
        print(f"  │{'─'*51}│")
        print(f"  │  Total: ₹{total_value:,.2f}  │  P&L: {pnl_sign}₹{total_pnl:,.2f}")
        print(f"  └─────────────────────────────────────────────────┘")
        print(f"  \033[92m✓ Session confirmed active (holdings API responded)\033[0m")

    except Exception as e:
        err_str = str(e).lower()
        if "403" in err_str or "session" in err_str or "unauthorized" in err_str:
            print(f"\n  \033[91m✗ Holdings failed: {e}\033[0m")
            print(f"  \033[91m  This may indicate login session is NOT active!\033[0m")
        else:
            print(f"  ⚠ Could not fetch holdings: {e}")


def _kotak_login(consumer_key, mobile_number, ucc, mpin):
    """
    Interactive Kotak Neo login with TOTP.
    Returns authenticated NeoAPI client or None.
    """
    try:
        from neo_api_client import NeoAPI
    except ImportError:
        print('  ✗ kotakneoapi not installed. Run:')
        print('    pip install kotakneoapi')
        return None

    # Import new exception types if available (v3.0+), fallback gracefully
    try:
        from neo_api_client import AuthenticationError
    except ImportError:
        AuthenticationError = Exception

    print(f"\n  ┌──────────────────────────────────────────────────┐")
    print(f"  │  KOTAK NEO LOGIN                                 │")
    print(f"  │  UCC: {ucc:<43}│")
    print(f"  │  Mobile: {mobile_number:<39}│")
    print(f"  └──────────────────────────────────────────────────┘")

    try:
        client = NeoAPI(
            environment='prod',
            access_token=None,
            neo_fin_key=None,
            consumer_key=consumer_key,
        )
        print(f"  ✓ NeoAPI client created")

        # Step 1: TOTP login
        totp = input(f"  Enter TOTP from authenticator app: ").strip()
        if not totp:
            print("  ✗ No TOTP entered.")
            return None

        try:
            resp = client.totp_login(
                mobilenumber=mobile_number,
                ucc=ucc,
                totp=totp,
            )
        except TypeError:
            # Some SDK versions use mobile_number instead
            resp = client.totp_login(
                mobile_number=mobile_number,
                ucc=ucc,
                totp=totp,
            )
        print(f"  ✓ TOTP login successful")

        # Step 2: Validate with MPIN
        resp = client.totp_validate(mpin=mpin)
        print(f"  ✓ MPIN validated — session active")
        print(f"  ✓ Kotak Neo ready for trading (₹0 brokerage on API orders)")

        # Show account info
        _show_kotak_profile(client)

        # Show open positions
        _show_kotak_positions(client)

        # Show holdings (confirms session is alive)
        _show_kotak_holdings(client)

        return client

    except Exception as e:
        print(f"  ✗ Kotak login failed: {e}")
        if "AuthenticationError" in type(e).__name__:
            print(f"    Check your consumer_key, mobile number, UCC, TOTP, and MPIN")
        return None


def _upstox_to_kotak_symbol(instrument_name, expiry_date=None):
    """
    Convert Upstox instrument name to Kotak trading symbol.

    Upstox name: "NIFTY 24300CE"  +  expiry "2026-08-26"
    Kotak symbol: "NIFTY26AUG24300CE"

    Upstox name: "BANKNIFTY 57500.50PE"  +  expiry "2026-08-25"
    Kotak symbol: "BANKNIFTY25AUG57500.50PE"

    Upstox name: "NIFTY FUT 2026-08-25"
    Kotak symbol: "NIFTY25AUGFUT"

    Strike format rules (Kotak):
      - Round strikes (24300) → "24300" (no .00)
      - Fractional strikes (57500.50) → "57500.50" (keep decimal)
    """
    import re
    from datetime import datetime as dt

    name = instrument_name.upper().strip()
    parts = name.split()

    if len(parts) < 2:
        return name  # can't parse, return as-is

    underlying = parts[0]  # NIFTY, BANKNIFTY, SENSEX

    # Case 1: Futures — "NIFTY FUT 2026-08-25"
    if "FUT" in name:
        # Extract expiry from name or use provided
        exp = None
        for p in parts:
            m = re.match(r'(\d{4}-\d{2}-\d{2})', p)
            if m:
                exp = m.group(1)
                break
        if not exp and expiry_date:
            exp = expiry_date

        if exp:
            try:
                d = dt.strptime(exp, "%Y-%m-%d")
                exp_str = d.strftime("%d%b").upper()  # "25AUG"
                return f"{underlying}{exp_str}FUT"
            except Exception:
                pass
        return f"{underlying}FUT"

    # Case 2: Options — "NIFTY 24300CE" or "BANKNIFTY 57500PE"
    strike_side = parts[1]  # "24300CE" or "24300.50CE"
    m = re.match(r'([\d.]+)(CE|PE)', strike_side)
    if m:
        strike_raw = m.group(1)
        side = m.group(2)

        # Build expiry string from explicit expiry_date
        exp_str = ""
        if expiry_date:
            try:
                d = dt.strptime(expiry_date, "%Y-%m-%d")
                exp_str = d.strftime("%d%b").upper()  # "26AUG"
            except Exception:
                pass

        if not exp_str:
            print(f"  ⚠ No expiry date for Kotak symbol — output will be incomplete")

        # Format strike: drop .00 for round numbers, keep .50 etc.
        try:
            strike_f = float(strike_raw)
            if strike_f == int(strike_f):
                strike_str = str(int(strike_f))        # 24300
            else:
                strike_str = f"{strike_f:.2f}".rstrip('0').rstrip('.')  # 57500.5 → "57500.5"
        except ValueError:
            strike_str = strike_raw

        # Format: NIFTY26AUG24300CE
        return f"{underlying}{exp_str}{strike_str}{side}"

    return name  # fallback


def _kotak_exchange_segment(instrument_key):
    """Map Upstox instrument key prefix to Kotak exchange segment."""
    if not instrument_key:
        return "nse_fo"
    key_upper = instrument_key.upper()
    if key_upper.startswith("NSE_FO"):
        return "nse_fo"
    elif key_upper.startswith("BSE_FO"):
        return "bse_fo"
    elif key_upper.startswith("MCX_FO"):
        return "mcx_fo"
    elif key_upper.startswith("NSE_CM") or key_upper.startswith("NSE_EQ"):
        return "nse_cm"
    elif key_upper.startswith("BSE_CM") or key_upper.startswith("BSE_EQ"):
        return "bse_cm"
    elif key_upper.startswith("CDE_FO"):
        return "cde_fo"
    return "nse_fo"


def _expiry_to_kotak_format(expiry_yyyy_mm_dd):
    """Convert '2026-08-25' → '25AUG2026' (format Kotak search_scrip expects)."""
    if not expiry_yyyy_mm_dd:
        return ""
    try:
        from datetime import datetime as dt
        d = dt.strptime(expiry_yyyy_mm_dd, "%Y-%m-%d")
        return d.strftime("%d%b%Y").upper()  # "25AUG2026"
    except Exception:
        return expiry_yyyy_mm_dd  # return as-is if can't parse


def _filter_exact_symbol(results, underlying):
    """Filter search_scrip results to only those whose pSymbolName exactly matches.
    
    search_scrip(symbol="NIFTY") returns BANKNIFTY too (substring match).
    This filter prevents NIFTY FUT from resolving to BANKNIFTY FUT.
    Falls back to pTrdSymbol prefix check if pSymbolName is missing.
    """
    if not results or not isinstance(results, list):
        return results

    exact = []
    underlying_upper = underlying.upper()
    for scrip in results:
        sym_name = (scrip.get("pSymbolName") or "").upper()
        trd_sym = (scrip.get("pTrdSymbol") or "").upper()

        # Primary: exact match on pSymbolName
        if sym_name == underlying_upper:
            exact.append(scrip)
        # Fallback: pTrdSymbol starts with underlying followed by a digit or month
        # e.g., "NIFTY25AUGFUT" starts with "NIFTY" + "2" (not a letter from "BANKNIFTY")
        elif not sym_name and trd_sym.startswith(underlying_upper):
            # Check that the character after underlying is NOT a letter
            # This prevents "NIFTY" matching "NIFTYNXT50" or similar
            rest = trd_sym[len(underlying_upper):]
            if rest and not rest[0].isalpha():
                exact.append(scrip)

    return exact if exact else results  # fall back to full list if nothing matched


def resolve_kotak_symbols(kotak_client, sub_keys, sub_meta):
    """
    Resolve all subscribed instruments to their Kotak trading symbols.
    Called at subscription time so user can verify mappings before streaming.
    Stores results in sub_meta[key]["kotak_symbol"].
    """
    import re
    if not kotak_client:
        return

    print(f"\n  ── Resolving Kotak Trading Symbols ──")
    resolved = 0

    for key in sub_keys:
        meta = sub_meta.get(key, {})
        name = meta.get("name", "")
        expiry = meta.get("expiry", "")
        # Convert expiry from Upstox format (2026-08-25) to Kotak format (25AUG2026)
        kotak_expiry = _expiry_to_kotak_format(expiry)

        if not name:
            continue

        # Skip indices (they're not tradeable on Kotak)
        upper = name.upper()
        if "FUT" not in upper and "CE" not in upper and "PE" not in upper:
            continue

        # First, build our best-guess symbol
        constructed = _upstox_to_kotak_symbol(name, expiry)

        # Now try search_scrip to validate / get exact symbol
        parts = upper.split()
        underlying = parts[0]
        exchange_seg = _kotak_exchange_segment(key)

        # Try to extract strike and option type
        if len(parts) >= 2:
            m = re.match(r'([\d.]+)(CE|PE)', parts[1])
        else:
            m = None

        kotak_sym = constructed  # fallback
        scrip_token = None

        if m:
            # Option
            strike_raw = m.group(1)
            option_type = m.group(2)
            try:
                strike_f = float(strike_raw)
                if strike_f == int(strike_f):
                    strike_str = str(int(strike_f))
                else:
                    strike_str = strike_raw
            except ValueError:
                strike_str = strike_raw

            try:
                result = kotak_client.search_scrip(
                    exchange_segment=exchange_seg,
                    symbol=underlying,
                    expiry=kotak_expiry,
                    option_type=option_type,
                    strike_price=strike_str,
                )
                # Filter to exact symbol match (prevents NIFTY→BANKNIFTY)
                result = _filter_exact_symbol(result, underlying)
                if result and isinstance(result, list) and len(result) > 0:
                    scrip = result[0]
                    ts = scrip.get("pTrdSymbol") or ""
                    tk = str(scrip.get("pSymbol") or "")  # v3: pSymbol is int
                    sym_name = scrip.get("pSymbolName") or ""
                    if ts:
                        kotak_sym = ts
                        scrip_token = tk
            except Exception as e:
                pass  # fall through to constructed symbol

        elif "FUT" in upper:
            # Future — v3 SDK: option_type="FUT" maps to pOptionType="XX" internally
            try:
                result = kotak_client.search_scrip(
                    exchange_segment=exchange_seg,
                    symbol=underlying,
                    expiry=kotak_expiry,
                    option_type="FUT",
                    strike_price="",
                )
                # Filter to exact symbol match (prevents NIFTY→BANKNIFTY)
                result = _filter_exact_symbol(result, underlying)
                if result and isinstance(result, list):
                    for scrip in result:
                        ts = scrip.get("pTrdSymbol") or ""
                        if "FUT" in ts.upper():
                            kotak_sym = ts
                            scrip_token = str(scrip.get("pSymbol") or "")
                            break
            except Exception:
                pass

        meta["kotak_symbol"] = kotak_sym
        if scrip_token:
            meta["kotak_token"] = scrip_token
        meta["kotak_exchange_segment"] = exchange_seg
        meta["kotak_verified"] = False  # will be set True after LTP check
        sub_meta[key] = meta

        # ── Token cross-check: Upstox and Kotak should use same NSE/BSE token ──
        upstox_token = key.split("|")[-1] if "|" in key else ""
        token_match = ""
        if scrip_token and upstox_token:
            if scrip_token == upstox_token:
                token_match = " tok✓"
            else:
                token_match = f" ⚠TOK MISMATCH(upstox={upstox_token},kotak={scrip_token})"
                meta["kotak_token_mismatch"] = True
                sub_meta[key] = meta

        match_tag = "✓" if scrip_token else "~"
        print(f"    {match_tag} {name:<28} → {kotak_sym}  token={scrip_token or '?'}{token_match}")
        resolved += 1

    if resolved == 0:
        print(f"    (no tradeable instruments to resolve)")
        return

    print(f"  Resolved {resolved} symbol(s)")

    # ── Verify by fetching LTP through Kotak Quotes API ──
    # v3 SDK: quotes() returns a list of dicts, each with "ltp" as string
    print(f"\n  ── Verifying via Kotak Quotes (LTP check) ──")
    verified = 0
    failed = 0
    for key in sub_keys:
        meta = sub_meta.get(key, {})
        scrip_token = meta.get("kotak_token", "")
        exchange_seg = meta.get("kotak_exchange_segment", "")
        kotak_sym = meta.get("kotak_symbol", "")
        name = meta.get("name", "")

        if not scrip_token or not exchange_seg:
            continue

        try:
            instrument_tokens = [{"instrument_token": scrip_token, "exchange_segment": exchange_seg}]
            quote_resp = kotak_client.quotes(instrument_tokens=instrument_tokens, quote_type="ltp")

            # v3 SDK returns a list: [{"ltp": "242.40", "display_symbol": "...", ...}]
            kotak_ltp = None
            if isinstance(quote_resp, list) and len(quote_resp) > 0:
                entry = quote_resp[0]
                if isinstance(entry, dict):
                    kotak_ltp = entry.get("ltp") or entry.get("last_price")
            elif isinstance(quote_resp, dict):
                # Fallback for older response format
                kotak_ltp = quote_resp.get("ltp")
                if not kotak_ltp:
                    for qval in quote_resp.values():
                        if isinstance(qval, dict):
                            kotak_ltp = qval.get("ltp") or qval.get("last_price")
                            if kotak_ltp:
                                break

            upstox_ltp = meta.get("ltp")
            if kotak_ltp is not None:
                kotak_ltp = float(kotak_ltp)
                ltp_str = f"₹{kotak_ltp}"
                if upstox_ltp:
                    diff_pct = abs(kotak_ltp - float(upstox_ltp)) / float(upstox_ltp) * 100
                    if diff_pct < 5:
                        print(f"    ✓ {kotak_sym:<30} LTP: {ltp_str}  (Upstox: ₹{upstox_ltp}, diff: {diff_pct:.1f}%)")
                    else:
                        print(f"    ⚠ {kotak_sym:<30} LTP: {ltp_str}  (Upstox: ₹{upstox_ltp}, diff: {diff_pct:.1f}% — MISMATCH?)")
                else:
                    print(f"    ✓ {kotak_sym:<30} LTP: {ltp_str}")
                verified += 1
            else:
                print(f"    ? {kotak_sym:<30} No LTP in response: {str(quote_resp)[:100]}")
                failed += 1
        except Exception as e:
            print(f"    ✗ {kotak_sym:<30} Quote failed: {e}")
            failed += 1

    if verified > 0:
        print(f"  ✓ {verified} symbol(s) verified via Kotak LTP")
    if failed > 0:
        print(f"  ⚠ {failed} symbol(s) could not be verified — review before trading!")

    # ── Extract lot sizes from search_scrip results (for auto-detection) ──
    # search_scrip returns lLotSize in some SDK versions
    # We also stored kotak_token which can be used to look up in scrip master
    # For now, mark verified status for each instrument
    for key in sub_keys:
        meta = sub_meta.get(key, {})
        scrip_token = meta.get("kotak_token", "")
        token_mismatch = meta.get("kotak_token_mismatch", False)

        if scrip_token and not token_mismatch:
            # Symbol was resolved via search_scrip AND token matches Upstox
            meta["kotak_verified"] = True
        else:
            meta["kotak_verified"] = False

        sub_meta[key] = meta


class KotakNeoExecutor(OrderExecutor):
    """
    Live order execution via Kotak Neo API (v3.0+ SDK).
    ₹0 brokerage on all API orders.

    SAFETY ARCHITECTURE:
      1. verified flag — blocks orders if symbol not confirmed via search_scrip
      2. _place_and_verify — places order, then confirms fill via order_report
      3. MIS time guard — blocks new buys after 3:10 PM IST
      4. Session error detection — warns on 403/timeout
      5. All failures return (FAILED_id, None) — buy()/sell() won't update state
    """
    def __init__(self, instrument_name, instrument_key="", lot_size=1, max_trades=50,
                 kotak_client=None, trading_symbol="", exchange_segment="nse_fo",
                 expiry_date=None):
        super().__init__(instrument_name, lot_size, max_trades)
        self.mode = ExecMode.LIVE
        self.instrument_key = instrument_key
        self.kotak_client = kotak_client
        self.exchange_segment = exchange_segment
        self.verified = False  # MUST be set True by resolve_kotak_symbols
        self._last_error = None
        self._last_error_time = None
        self._consecutive_errors = 0
        self._trading_paused = False  # set True after repeated failures

        if trading_symbol:
            self.trading_symbol = trading_symbol
        else:
            self.trading_symbol = _upstox_to_kotak_symbol(instrument_name, expiry_date)
            print(f"  Kotak symbol: {self.trading_symbol}  (will verify at stream start)")

    def _check_order_status(self, order_id):
        """
        Quick order verification via order_report(). ONE attempt only
        to minimize tick-loop blocking (~200ms instead of 1.5s).
        
        For MKT orders on liquid instruments, the exchange fills in <10ms.
        By the time our HTTP call returns, the order is already filled.
        If status is still "pending" after 1 check, we treat it as
        "likely filled" for MKT orders and use LTP as fill price.
        
        Returns: ("complete", fill_price, fill_qty)
                 ("accepted", None, None)        — pending but likely filled (MKT)
                 ("rejected", None, None)
                 ("error", None, None)
        """
        if not order_id or not self.kotak_client:
            return ("error", None, None)

        try:
            report = self.kotak_client.order_report()
            if not report:
                return ("accepted", None, None)  # can't check, assume MKT filled

            orders = []
            if isinstance(report, dict):
                orders = report.get("data", [])
            elif isinstance(report, list):
                orders = report

            for order in orders:
                if not isinstance(order, dict):
                    continue
                oid = str(order.get("nOrdNo") or order.get("ordNo") or "")
                if oid != str(order_id):
                    continue

                # Found our order
                status = (order.get("ordSt") or order.get("status") or "").lower()
                rej_reason = order.get("rejRsn") or order.get("rejectionReason") or ""

                if status in ("complete", "traded", "filled"):
                    fill_price = None
                    fill_qty = None
                    avg_prc = order.get("avgPrc") or order.get("fldAvgPrc") or ""
                    if avg_prc:
                        try:
                            fill_price = float(avg_prc)
                        except (ValueError, TypeError):
                            pass
                    fld_qty = order.get("fldQty") or order.get("flQty") or ""
                    if fld_qty:
                        try:
                            fill_qty = int(fld_qty)
                        except (ValueError, TypeError):
                            pass
                    return ("complete", fill_price, fill_qty)

                elif status in ("rejected", "cancelled", "canceled"):
                    print(f"  \033[91m[KOTAK] Order {order_id} {status.upper()}: {rej_reason}\033[0m")
                    return ("rejected", None, None)

                else:
                    # "open", "pending", etc. — for MKT orders, this is transient
                    return ("accepted", None, None)

            # Order not found in report — might be too new
            return ("accepted", None, None)

        except Exception as e:
            print(f"  \033[93m[KOTAK] order_report check: {e}\033[0m")
            return ("accepted", None, None)  # assume MKT filled, reconcile later

    def _execute_buy(self, price, timestamp):
        """Place a BUY MKT order synchronously (~200ms), then verify in background thread.
        Tick loop resumes immediately. Verification updates entry_price with actual fill."""
        if not self.kotak_client:
            return (None, price)

        if self._trading_paused:
            return (f"BLOCKED_PAUSED_{timestamp}", None)

        if not self.verified:
            self._set_error("Symbol not verified — orders blocked")
            return (f"BLOCKED_{timestamp}", None)

        now_ist = datetime.now(IST)
        if now_ist.hour > 15 or (now_ist.hour == 15 and now_ist.minute >= 10):
            self._set_error("Past 3:10 PM — no new MIS entries")
            return (f"BLOCKED_TIME_{timestamp}", None)

        try:
            print(f"  \033[92m[KOTAK BUY] {self.trading_symbol} qty={self.lot_size} MKT @ ~₹{price}\033[0m")
            order_kwargs = dict(
                exchange_segment=self.exchange_segment,
                product="MIS", price="0", order_type="MKT",
                quantity=str(self.lot_size), validity="DAY",
                trading_symbol=self.trading_symbol,
                transaction_type="B", amo="NO",
                disclosed_quantity="0", trigger_price="0",
                tag="ALGO_BUY",
            )
            try:
                resp = self.kotak_client.place_order(**order_kwargs)
            except TypeError:
                # Older SDK doesn't support tag — retry without it
                order_kwargs.pop("tag", None)
                resp = self.kotak_client.place_order(**order_kwargs)

            order_id = None
            if isinstance(resp, dict):
                order_id = resp.get("nOrdNo") or resp.get("order_id")
                stat = (resp.get("stat") or "").lower()
                if stat in ("error", "failed", "not_ok") or resp.get("stCode", 200) >= 400:
                    err = resp.get("errMsg") or resp.get("message") or str(resp)
                    print(f"  \033[91m[KOTAK] BUY REJECTED: {err}\033[0m")
                    self._set_error(f"BUY rejected: {err}")
                    return (f"REJECTED_{timestamp}", None)
            elif hasattr(resp, 'nOrdNo'):
                order_id = resp.nOrdNo

            if not order_id:
                self._set_error("BUY failed — no order_id")
                return (f"ERROR_{timestamp}", None)

            print(f"  \033[92m[KOTAK] BUY ACCEPTED ✓ OID:{order_id} → entering @ ₹{price}\033[0m")
            self._clear_error()

            # ── Background thread: verify fill + get actual price ──
            self._spawn_verify_thread(order_id, "BUY", price)

            return (order_id, price)

        except Exception as e:
            err_str = str(e)
            print(f"  \033[91m[KOTAK] BUY exception: {err_str}\033[0m")
            if "403" in err_str or "session" in err_str.lower():
                self._set_error("SESSION EXPIRED — all orders will fail!")
            else:
                self._set_error(f"BUY exception: {err_str[:60]}")
            return (f"ERROR_{timestamp}", None)

    def _execute_sell(self, price, timestamp):
        """Place a SELL MKT order synchronously, verify in background thread."""
        if not self.kotak_client:
            return (None, price)

        # Don't pause sells — we NEED to exit open positions even after errors
        if not self.verified:
            self._set_error("SELL blocked — symbol not verified, POSITION STILL OPEN!")
            return (f"BLOCKED_{timestamp}", None)

        try:
            pnl_est = ""
            if self.entry_price:
                p = (price - self.entry_price) * self.lot_size
                pnl_est = f" est P&L:{'+'if p>=0 else ''}₹{p:.0f}"
            print(f"  \033[91m[KOTAK SELL] {self.trading_symbol} qty={self.lot_size} MKT @ ~₹{price}{pnl_est}\033[0m")

            order_kwargs = dict(
                exchange_segment=self.exchange_segment,
                product="MIS", price="0", order_type="MKT",
                quantity=str(self.lot_size), validity="DAY",
                trading_symbol=self.trading_symbol,
                transaction_type="S", amo="NO",
                disclosed_quantity="0", trigger_price="0",
                tag="ALGO_SELL",
            )
            try:
                resp = self.kotak_client.place_order(**order_kwargs)
            except TypeError:
                order_kwargs.pop("tag", None)
                resp = self.kotak_client.place_order(**order_kwargs)

            order_id = None
            if isinstance(resp, dict):
                order_id = resp.get("nOrdNo") or resp.get("order_id")
                stat = (resp.get("stat") or "").lower()
                if stat in ("error", "failed", "not_ok") or resp.get("stCode", 200) >= 400:
                    err = resp.get("errMsg") or resp.get("message") or str(resp)
                    print(f"  \033[91m[KOTAK] SELL REJECTED: {err}\033[0m")
                    print(f"  \033[91m  ⚠ POSITION STILL OPEN — close in Kotak app!\033[0m")
                    self._set_error(f"SELL REJECTED — POSITION OPEN!")
                    return (f"REJECTED_{timestamp}", None)
            elif hasattr(resp, 'nOrdNo'):
                order_id = resp.nOrdNo

            if not order_id:
                self._set_error("SELL failed — POSITION OPEN!")
                return (f"ERROR_{timestamp}", None)

            print(f"  \033[91m[KOTAK] SELL ACCEPTED ✓ OID:{order_id} → exiting @ ₹{price}\033[0m")
            self._clear_error()

            # ── Background thread: verify fill + log actual P&L ──
            self._spawn_verify_thread(order_id, "SELL", price)

            return (order_id, price)

        except Exception as e:
            err_str = str(e)
            print(f"  \033[91m[KOTAK] SELL exception: {err_str}\033[0m")
            if "403" in err_str or "session" in err_str.lower():
                print(f"  \033[91m  ⚠ SESSION EXPIRED — POSITION OPEN! Close in Kotak app!\033[0m")
                self._set_error("SESSION EXPIRED — POSITION OPEN!")
            else:
                self._set_error(f"SELL exception — POSITION MAY BE OPEN!")
            return (f"ERROR_{timestamp}", None)

    def _set_error(self, msg):
        """Set a persistent error. After 3 consecutive failures, pause trading."""
        self._last_error = msg
        self._last_error_time = datetime.now(IST)
        self._consecutive_errors += 1
        if self._consecutive_errors >= 3 and not self._trading_paused:
            self._trading_paused = True
            print(f"\n  \033[91m{'━'*60}\033[0m")
            print(f"  \033[91m  ⚠ TRADING PAUSED — {self._consecutive_errors} consecutive failures\033[0m")
            print(f"  \033[91m  Last error: {msg}\033[0m")
            print(f"  \033[91m  Check: account balance, Kotak session, instrument validity\033[0m")
            print(f"  \033[91m  Strategy continues tracking but no orders will be placed.\033[0m")
            print(f"  \033[91m{'━'*60}\033[0m")

    def _clear_error(self):
        """Clear error state after successful operation. Unpauses trading."""
        self._last_error = None
        self._last_error_time = None
        self._consecutive_errors = 0
        if self._trading_paused:
            self._trading_paused = False
            print(f"  \033[92m  ✓ Trading resumed — order succeeded\033[0m")

    def _spawn_verify_thread(self, order_id, side, expected_price):
        """Spawn a daemon thread to verify the order fill and update state.
        Runs in background — does NOT block the tick loop."""
        def _verify():
            time.sleep(1)  # give exchange a moment (fills in <10ms but API might lag)
            try:
                status, fill_price, fill_qty = self._check_order_status(order_id)

                if status == "complete":
                    actual = fill_price or expected_price
                    slippage = actual - expected_price if fill_price else 0
                    qty_str = f" qty={fill_qty}" if fill_qty else ""
                    slip_str = f" slip=₹{slippage:+.2f}" if abs(slippage) > 0.01 else ""
                    print(f"  \033[96m[VERIFY ✓] {side} OID:{order_id} FILLED @ ₹{actual}{qty_str}{slip_str}\033[0m")

                    # Update entry_price with actual fill price if we're still in that position
                    if fill_price and side == "BUY" and self.in_position:
                        if abs(fill_price - self.entry_price) > 0.01:
                            self.entry_price = fill_price
                            self.peak_since_entry = max(self.peak_since_entry or 0, fill_price)

                elif status == "rejected":
                    print(f"  \033[91m[VERIFY ✗] {side} OID:{order_id} REJECTED after acceptance!\033[0m")
                    if side == "BUY" and self.in_position:
                        print(f"  \033[91m  ⚠ We think we're in position but order was REJECTED!\033[0m")
                        print(f"  \033[91m  ⚠ Check Kotak app — strategy state may be WRONG!\033[0m")
                        self._set_error(f"{side} {order_id} REJECTED after acceptance!")
                    elif side == "SELL":
                        print(f"  \033[91m  ⚠ SELL rejected — POSITION MAY STILL BE OPEN!\033[0m")
                        self._set_error(f"SELL {order_id} REJECTED — check Kotak app!")

                elif status == "accepted":
                    # Still pending after 1s — unusual for MKT, retry once more
                    time.sleep(2)
                    status2, fill_price2, _ = self._check_order_status(order_id)
                    if status2 == "complete" and fill_price2:
                        print(f"  \033[96m[VERIFY ✓] {side} OID:{order_id} FILLED @ ₹{fill_price2} (delayed)\033[0m")
                        if side == "BUY" and self.in_position and abs(fill_price2 - self.entry_price) > 0.01:
                            self.entry_price = fill_price2
                    elif status2 == "rejected":
                        print(f"  \033[91m[VERIFY ✗] {side} OID:{order_id} REJECTED!\033[0m")
                        self._set_error(f"{side} {order_id} REJECTED!")
                    else:
                        print(f"  \033[93m[VERIFY ?] {side} OID:{order_id} status={status2} — check Kotak app\033[0m")

            except Exception as e:
                print(f"  \033[93m[VERIFY] {side} OID:{order_id} check failed: {e}\033[0m")

        t = threading.Thread(target=_verify, daemon=True, name=f"verify_{side}_{order_id}")
        t.start()

    def status_line(self, strategy=None):
        """Override to include error state."""
        base = super().status_line(strategy)
        if self._trading_paused:
            base += f"\n  \033[91m  ⚠ TRADING PAUSED ({self._consecutive_errors} errors) — {self._last_error}\033[0m"
        elif self._last_error:
            base += f"\n  \033[91m  ⚠ ERROR: {self._last_error}\033[0m"
        return base


# ═══════════════════════════════════════════════════════════════
# STRATEGY RUNNERS — Use OrderExecutor instead of PaperTrader
# ═══════════════════════════════════════════════════════════════
class ExecutionStrategyRunner:
    """Single-instrument strategy runner with executor."""
    def __init__(self, strategy, executor, instrument_key, future_key=None):
        self.strategy = strategy
        self.trader = executor  # OrderExecutor (compatible with PaperTrader interface)
        self.instrument_key = instrument_key
        self.future_key = future_key
        self._last_status_tick = [0]

    def on_tick(self, key, ltp, timestamp, feed_data=None):
        if ltp is None:
            return
        try:
            self._process_tick(key, ltp, timestamp, feed_data)
        except Exception as e:
            import traceback
            print(f"  \033[91m[STRATEGY ERROR] {e}\033[0m")
            traceback.print_exc()

    def _process_tick(self, key, ltp, timestamp, feed_data=None):
        if key == self.instrument_key:
            self.trader.update_tick(ltp, timestamp)
            if hasattr(self.strategy, 'on_option_tick'):
                signal = self.strategy.on_option_tick(ltp, timestamp, feed_data)
            else:
                signal = self.strategy.on_tick(ltp, timestamp, feed_data)
            if signal == "BUY" and self.trader.can_buy():
                success = self.trader.buy(ltp, timestamp)
                if not success:
                    # BUY was rejected/blocked — reset strategy state to prevent deadlock
                    # Strategy already set _in_position=True internally when it returned "BUY"
                    # We must undo that, otherwise it will never signal BUY again
                    self._reset_strategy_position(self.strategy)
                elif self.trader.entry_price and hasattr(self.strategy, 'entry_price'):
                    # Sync strategy with actual fill price from broker
                    fill = self.trader.entry_price
                    if self.strategy.entry_price and abs(fill - self.strategy.entry_price) > 0.01:
                        self.strategy.entry_price = fill
                        self.strategy.peak_price = max(self.strategy.peak_price or fill, fill)
                        trail = self.strategy._get_trail_distance(0)
                        self.strategy.stop_loss = fill - trail
            elif signal == "SELL" and self.trader.can_sell():
                success = self.trader.sell(ltp, timestamp)
                if not success:
                    # SELL failed — strategy will keep trying on next tick (correct behavior)
                    print(f"  \033[91m[RUNNER] SELL failed — will retry on next stop-loss hit\033[0m")
            if self.strategy.tick_count - self._last_status_tick[0] >= 200:
                self._last_status_tick[0] = self.strategy.tick_count
                print(f"  {self.trader.status_line(self.strategy)}")
        elif self.future_key and key == self.future_key:
            if hasattr(self.strategy, 'on_future_tick'):
                self.strategy.on_future_tick(ltp, timestamp, feed_data)

    @staticmethod
    def _reset_strategy_position(strategy):
        """Reset strategy's internal position state after a rejected BUY.
        Without this, the strategy thinks it's in position and deadlocks."""
        if hasattr(strategy, '_in_position'):
            strategy._in_position = False
            strategy.entry_price = None
            strategy.peak_price = None
            strategy.stop_loss = None
            strategy._breakeven_active = False
            strategy._breakeven_stop = None
            print(f"  \033[93m[RUNNER] Strategy position state reset (BUY was rejected)\033[0m")

    def print_final_summary(self):
        print(f"\n  {'═'*60}")
        print(f"  STRATEGY SUMMARY — {self.strategy.name} [{self.trader.mode.value.upper()}]")
        print(f"  {'═'*60}")
        self.strategy.describe()
        print(f"  Execution mode: {self.trader.mode.value}")
        print(f"  Ticks processed: {self.strategy.tick_count:,}")
        self.trader.print_trade_table()
        if self.trader.in_position:
            ur_pnl, ur_pct = self.trader.unrealized_pnl()
            sign = "+" if ur_pnl >= 0 else ""
            print(f"  ⚠ OPEN POSITION: Entry ₹{self.trader.entry_price:.2f}, "
                  f"LTP ₹{self.trader.current_ltp:.2f}, "
                  f"Unrealized {sign}₹{ur_pnl:.2f} ({sign}{ur_pct:.1f}%)")
        print(f"  {'═'*60}")


class ExecutionDualRunner:
    """Dual CE+PE runner with executors."""
    def __init__(self, call_strategy, call_executor, call_key,
                 put_strategy, put_executor, put_key, future_key):
        self.call_strat = call_strategy
        self.call_trader = call_executor
        self.call_key = call_key
        self.put_strat = put_strategy
        self.put_trader = put_executor
        self.put_key = put_key
        self.future_key = future_key
        self._last_call_status = [0]
        self._last_put_status = [0]
        # Compatibility with stream()
        self.strategy = call_strategy
        self.trader = call_executor
        self.instrument_key = call_key

    def on_tick(self, key, ltp, timestamp, feed_data=None):
        if ltp is None:
            return
        try:
            self._process_tick(key, ltp, timestamp, feed_data)
        except Exception as e:
            import traceback
            print(f"  \033[91m[DUAL STRATEGY ERROR] {e}\033[0m")
            traceback.print_exc()

    def _process_tick(self, key, ltp, timestamp, feed_data=None):
        if key == self.call_key:
            self.call_trader.update_tick(ltp, timestamp)
            signal = self.call_strat.on_option_tick(ltp, timestamp, feed_data)
            if signal == "BUY" and self.call_trader.can_buy():
                if not self.call_trader.buy(ltp, timestamp):
                    ExecutionStrategyRunner._reset_strategy_position(self.call_strat)
                elif self.call_trader.entry_price and hasattr(self.call_strat, 'entry_price'):
                    fill = self.call_trader.entry_price
                    if self.call_strat.entry_price and abs(fill - self.call_strat.entry_price) > 0.01:
                        self.call_strat.entry_price = fill
                        self.call_strat.peak_price = max(self.call_strat.peak_price or fill, fill)
                        trail = self.call_strat._get_trail_distance(0)
                        self.call_strat.stop_loss = fill - trail
            elif signal == "SELL" and self.call_trader.can_sell():
                if not self.call_trader.sell(ltp, timestamp):
                    print(f"  \033[91m[CE] SELL failed — will retry\033[0m")
            if self.call_strat.tick_count - self._last_call_status[0] >= 200:
                self._last_call_status[0] = self.call_strat.tick_count
                print(f"  \033[36m[CE] {self.call_trader.status_line(self.call_strat)}\033[0m")
        elif key == self.put_key:
            self.put_trader.update_tick(ltp, timestamp)
            signal = self.put_strat.on_option_tick(ltp, timestamp, feed_data)
            if signal == "BUY" and self.put_trader.can_buy():
                if not self.put_trader.buy(ltp, timestamp):
                    ExecutionStrategyRunner._reset_strategy_position(self.put_strat)
                elif self.put_trader.entry_price and hasattr(self.put_strat, 'entry_price'):
                    fill = self.put_trader.entry_price
                    if self.put_strat.entry_price and abs(fill - self.put_strat.entry_price) > 0.01:
                        self.put_strat.entry_price = fill
                        self.put_strat.peak_price = max(self.put_strat.peak_price or fill, fill)
                        trail = self.put_strat._get_trail_distance(0)
                        self.put_strat.stop_loss = fill - trail
            elif signal == "SELL" and self.put_trader.can_sell():
                if not self.put_trader.sell(ltp, timestamp):
                    print(f"  \033[91m[PE] SELL failed — will retry\033[0m")
            if self.put_strat.tick_count - self._last_put_status[0] >= 200:
                self._last_put_status[0] = self.put_strat.tick_count
                print(f"  \033[35m[PE] {self.put_trader.status_line(self.put_strat)}\033[0m")
        elif key == self.future_key:
            self.call_strat.on_future_tick(ltp, timestamp, feed_data)
            self.put_strat.on_future_tick(ltp, timestamp, feed_data)

    def print_final_summary(self):
        for label, strat, trader in [("CALL (CE)", self.call_strat, self.call_trader),
                                     ("PUT (PE)", self.put_strat, self.put_trader)]:
            print(f"\n  {'═'*60}")
            print(f"  {label} SUMMARY — {strat.name} [{trader.mode.value.upper()}]")
            print(f"  {'═'*60}")
            strat.describe()
            print(f"  Execution mode: {trader.mode.value}")
            print(f"  Ticks processed: {strat.tick_count:,}")
            trader.print_trade_table()
            if trader.in_position:
                ur_pnl, ur_pct = trader.unrealized_pnl()
                sign = "+" if ur_pnl >= 0 else ""
                print(f"  ⚠ OPEN POSITION: Entry ₹{trader.entry_price:.2f}, "
                      f"LTP ₹{trader.current_ltp:.2f}, "
                      f"Unrealized {sign}₹{ur_pnl:.2f} ({sign}{ur_pct:.1f}%)")
            print(f"  {'═'*60}")


# ═══════════════════════════════════════════════════════════════
# EXECUTOR FACTORY
# ═══════════════════════════════════════════════════════════════
def create_executor(mode, instrument_name, instrument_key=None, lot_size=1,
                    max_trades=50, sandbox_token=None, trading_symbol=None,
                    kotak_client=None, expiry_date=None, sub_meta=None):
    """Create the right executor based on mode.

    For Kotak live mode, pulls pre-resolved trading_symbol from sub_meta
    (set by resolve_kotak_symbols at subscription time).
    """
    if mode == ExecMode.PAPER:
        return PaperExecutor(instrument_name, lot_size, max_trades)

    elif mode == ExecMode.SANDBOX:
        if not sandbox_token:
            print("  ⚠ No sandbox token provided. Falling back to paper mode.")
            return PaperExecutor(instrument_name, lot_size, max_trades)
        executor = UpstoxSandboxExecutor(
            instrument_name, instrument_key, lot_size, max_trades, sandbox_token
        )
        executor.validate()
        return executor

    elif mode == ExecMode.LIVE:
        if not kotak_client:
            print("  ⚠ Kotak not logged in. Falling back to paper mode.")
            return PaperExecutor(instrument_name, lot_size, max_trades)

        # Pull pre-resolved symbol from sub_meta if available
        if not trading_symbol and sub_meta and instrument_key in sub_meta:
            trading_symbol = sub_meta[instrument_key].get("kotak_symbol", "")
            expiry_date = expiry_date or sub_meta[instrument_key].get("expiry", "")

        exchange_seg = _kotak_exchange_segment(instrument_key)
        return KotakNeoExecutor(
            instrument_name, instrument_key, lot_size, max_trades,
            kotak_client=kotak_client,
            trading_symbol=trading_symbol or "",
            exchange_segment=exchange_seg,
            expiry_date=expiry_date,
        )

    else:
        print(f"  ⚠ Unknown mode '{mode}'. Using paper.")
        return PaperExecutor(instrument_name, lot_size, max_trades)


# ═══════════════════════════════════════════════════════════════
# STRATEGY SETUP — Modified to use executors
# ═══════════════════════════════════════════════════════════════
def setup_execution_strategy(sub_keys, sub_meta, exec_mode, sandbox_token=None,
                             kotak_client=None, client=None):
    """Interactive strategy setup with execution mode. Returns runner or None.
    sub_meta may contain 'kotak_symbol' and 'expiry' keys (set by resolve_kotak_symbols)."""
    print(f"\n  ── Available Strategies ──")
    for sid, info in STRATEGIES.items():
        print(f"    {sid}. {info['name']}")
    print(f"    0. No strategy (just stream)")

    pick = input(f"\n  Pick strategy: ").strip()
    if pick == "0" or pick not in STRATEGIES:
        return None

    is_dual = (pick == "3")

    if not is_dual:
        strategy = STRATEGIES[pick]["class"]()
        strategy.configure()
    else:
        print(f"\n  Dual CE+PE Momentum — same parameters for both sides.")
        temp = OptionFutureMomentum(side="CALL")
        temp.configure()

    # Pick instrument
    print(f"\n  Which option should the strategy trade?")
    for i, k in enumerate(sub_keys, 1):
        meta = sub_meta.get(k, {})
        name = meta.get("name", k.split("|")[-1])
        print(f"    {i}. {name:<28} {k}")

    inst_pick = input(f"\n  Pick [1-{len(sub_keys)}]: ").strip()
    try:
        inst_idx = int(inst_pick) - 1
        inst_key = sub_keys[inst_idx]
    except (ValueError, IndexError):
        print("  Invalid. No strategy set.")
        return None

    inst_name = sub_meta.get(inst_key, {}).get("name", inst_key.split("|")[-1])
    underlying = inst_name.split()[0] if inst_name else ""

    # Find future
    future_key = None
    if pick in ("2", "3"):
        print(f"\n  Strategy needs the related FUTURE for {inst_name}.")
        future_key = _find_future(underlying, inst_key, sub_keys, sub_meta, client)
        if future_key:
            print(f"  Future key: {future_key}")
        else:
            print(f"  ⚠ No future found.")

    # Lot size and max trades
    # Known lot sizes for common indices
    KNOWN_LOT_SIZES = {
        "NIFTY": 65, "BANKNIFTY": 30, "FINNIFTY": 60, "MIDCPNIFTY": 120,
        "SENSEX": 20, "BANKEX": 30, "SENSEX50": 75, "NIFTYNXT50": 75,
    }
    exchange_lot = KNOWN_LOT_SIZES.get(underlying.upper(), None)

    if exec_mode == ExecMode.LIVE and exchange_lot:
        print(f"\n  Exchange lot size for {underlying}: {exchange_lot} shares")
        print(f"  Kotak requires quantity in SHARES (multiples of {exchange_lot})")
        num_lots = input(f"  How many lots? [1]: ").strip()
        num_lots = int(num_lots) if num_lots else 1
        lot_size = exchange_lot * num_lots
        print(f"  → Order quantity: {num_lots} lot(s) × {exchange_lot} = {lot_size} shares")
    elif exec_mode == ExecMode.LIVE:
        print(f"\n  ⚠ Unknown lot size for {underlying}.")
        print(f"  Enter quantity in SHARES (must be a multiple of exchange lot size):")
        lot_input = input(f"  Quantity in shares [1]: ").strip()
        lot_size = int(lot_input) if lot_input else 1
    else:
        lot_input = input(f"\n  Lot size [{1}]: ").strip()
        lot_size = int(lot_input) if lot_input else 1

    max_input = input(f"  Max trades per side [{50}]: ").strip()
    max_trades = int(max_input) if max_input else 50

    # ── Single side ──
    if not is_dual:
        if isinstance(strategy, OptionFutureMomentum):
            # Detect side from instrument name
            if "PE" in inst_name.upper():
                strategy.side = "PUT"
                strategy.name = strategy.name.replace("CALL", "PUT")
                print(f"  ✓ Detected PE option → strategy set to PUT side")
            elif "CE" in inst_name.upper():
                strategy.side = "CALL"
                print(f"  ✓ Detected CE option → strategy set to CALL side")
            strategy.future_key = future_key

        executor = create_executor(
            exec_mode, inst_name, inst_key, lot_size, max_trades, sandbox_token,
            kotak_client=kotak_client, sub_meta=sub_meta,
        )
        runner = ExecutionStrategyRunner(strategy, executor, inst_key, future_key=future_key)
        print(f"\n  ✓ Strategy '{strategy.name}' on {inst_name} [{exec_mode.value}]")
        strategy.describe()
        return runner

    # ── Dual CE+PE ──
    call_key, call_name = inst_key, inst_name
    put_key, put_name = None, None

    if "CE" in inst_name.upper():
        call_key, call_name = inst_key, inst_name
        print(f"\n  You picked CE. Finding the matching PE...")
        put_key, put_name = _find_counterpart_option(inst_name, inst_key, sub_keys, sub_meta, client)

        # Confirm or pick a different PE
        if put_key and put_name:
            put_ltp = sub_meta.get(put_key, {}).get("ltp", "?")
            call_ltp = sub_meta.get(call_key, {}).get("ltp", "?")
            print(f"\n  ┌─── Dual Pairing ─────────────────────────────────────┐")
            print(f"  │  CE: {call_name:<30} LTP: ₹{str(call_ltp or '?'):<8}│")
            print(f"  │  PE: {put_name:<30} LTP: ₹{str(put_ltp or '?'):<8}│")
            print(f"  └─────────────────────────────────────────────────────┘")
            # Warn if either leg is below ₹100
            warnings = []
            try:
                if call_ltp and float(call_ltp) < 100:
                    warnings.append(f"CE at ₹{call_ltp} is below ₹100 — too cheap for this strategy")
            except (ValueError, TypeError): pass
            try:
                if put_ltp and float(put_ltp) < 100:
                    warnings.append(f"PE at ₹{put_ltp} is below ₹100 — too cheap for this strategy")
            except (ValueError, TypeError): pass
            if warnings:
                print(f"  \033[93m⚠ WARNING — strategy works best with options ₹100+\033[0m")
                for w in warnings:
                    print(f"  \033[93m  • {w}\033[0m")
                print(f"  \033[93m  Consider picking different strikes or a closer expiry.\033[0m")
            confirm = input(f"  Use this PE? [Y/n] or pick another: ").strip().lower()
            if confirm in ("n", "no"):
                # Show subscribed PE options to pick from
                pe_options = [(k, sub_meta[k].get("name", k)) for k in sub_keys
                              if "PE" in sub_meta.get(k, {}).get("name", "").upper()]
                if pe_options:
                    print(f"\n  Available PE options in subscription:")
                    for i, (k, n) in enumerate(pe_options, 1):
                        pe_ltp = sub_meta.get(k, {}).get("ltp", "?")
                        print(f"    {i}. {n:<30} ₹{pe_ltp}  {k}")
                    pick = input(f"  Pick PE [1-{len(pe_options)}]: ").strip()
                    try:
                        idx = int(pick) - 1
                        if 0 <= idx < len(pe_options):
                            put_key, put_name = pe_options[idx]
                            print(f"  ✓ Using: {put_name}")
                        else:
                            print(f"  Invalid pick. Using auto-matched PE.")
                    except ValueError:
                        print(f"  Using auto-matched PE.")
                else:
                    print(f"  No other PE options subscribed. Add PE via option 1 first,")
                    print(f"  then re-configure strategy (option 10).")
                    print(f"  Using auto-matched PE for now.")

    elif "PE" in inst_name.upper():
        put_key, put_name = inst_key, inst_name
        print(f"\n  You picked PE. Finding the matching CE...")
        call_key, call_name = _find_counterpart_option(inst_name, inst_key, sub_keys, sub_meta, client)

        # Confirm or pick a different CE
        if call_key and call_name:
            call_ltp = sub_meta.get(call_key, {}).get("ltp", "?")
            put_ltp = sub_meta.get(put_key, {}).get("ltp", "?")
            print(f"\n  ┌─── Dual Pairing ─────────────────────────────────────┐")
            print(f"  │  CE: {call_name:<30} LTP: ₹{str(call_ltp or '?'):<8}│")
            print(f"  │  PE: {put_name:<30} LTP: ₹{str(put_ltp or '?'):<8}│")
            print(f"  └─────────────────────────────────────────────────────┘")
            confirm = input(f"  Use this CE? [Y/n] or pick another: ").strip().lower()
            if confirm in ("n", "no"):
                ce_options = [(k, sub_meta[k].get("name", k)) for k in sub_keys
                              if "CE" in sub_meta.get(k, {}).get("name", "").upper()]
                if ce_options:
                    print(f"\n  Available CE options in subscription:")
                    for i, (k, n) in enumerate(ce_options, 1):
                        ce_ltp = sub_meta.get(k, {}).get("ltp", "?")
                        print(f"    {i}. {n:<30} ₹{ce_ltp}  {k}")
                    pick = input(f"  Pick CE [1-{len(ce_options)}]: ").strip()
                    try:
                        idx = int(pick) - 1
                        if 0 <= idx < len(ce_options):
                            call_key, call_name = ce_options[idx]
                            print(f"  ✓ Using: {call_name}")
                    except ValueError:
                        pass
    else:
        print(f"  ⚠ Can't determine CE/PE. Using as CALL side.")

    if not call_key or not put_key:
        print(f"  ⚠ Could not find both CE and PE. Falling back to single-side.")
        strategy = OptionFutureMomentum(side="CALL")
        strategy.opt_low_margin = temp.opt_low_margin
        strategy.fut_low_margin = temp.fut_low_margin
        strategy.trail_tiers = temp.trail_tiers
        strategy.trail_mode = temp.trail_mode
        strategy.breakeven_trigger_pct = temp.breakeven_trigger_pct
        strategy.breakeven_buffer = temp.breakeven_buffer
        strategy.straight_down_stop_pct = temp.straight_down_stop_pct
        strategy.fut_move_limit = temp.fut_move_limit
        strategy.opt_open_rally_pct = temp.opt_open_rally_pct
        strategy.opt_atp_distance_pct = temp.opt_atp_distance_pct
        strategy.opt_buy_sell_ratio = temp.opt_buy_sell_ratio
        strategy.bev_buffer_mode = temp.bev_buffer_mode
        strategy.rally_base = temp.rally_base
        strategy.minute_interval = temp.minute_interval
        strategy.opt_momentum_threshold = temp.opt_momentum_threshold
        strategy.require_lower_wick = temp.require_lower_wick
        strategy.block_marubozu = temp.block_marubozu
        strategy.max_range = temp.max_range
        strategy.max_momentum = temp.max_momentum
        strategy.max_above_high = temp.max_above_high
        strategy.rebuy_drop = temp.rebuy_drop
        strategy.max_hold = temp.max_hold
        strategy.target_profit = temp.target_profit
        strategy.first_only = temp.first_only
        strategy.future_key = future_key
        executor = create_executor(exec_mode, inst_name, inst_key, lot_size, max_trades, sandbox_token,
                                    kotak_client=kotak_client, sub_meta=sub_meta)
        return ExecutionStrategyRunner(strategy, executor, inst_key, future_key=future_key)

    # Create strategies
    call_strat = OptionFutureMomentum(side="CALL")
    call_strat.opt_low_margin = temp.opt_low_margin
    call_strat.fut_low_margin = temp.fut_low_margin
    call_strat.trail_tiers = list(temp.trail_tiers)
    call_strat.trail_mode = temp.trail_mode
    call_strat.breakeven_trigger_pct = temp.breakeven_trigger_pct
    call_strat.breakeven_buffer = temp.breakeven_buffer
    call_strat.straight_down_stop_pct = temp.straight_down_stop_pct
    call_strat.fut_move_limit = temp.fut_move_limit
    call_strat.opt_open_rally_pct = temp.opt_open_rally_pct
    call_strat.opt_atp_distance_pct = temp.opt_atp_distance_pct
    call_strat.opt_buy_sell_ratio = temp.opt_buy_sell_ratio
    call_strat.bev_buffer_mode = temp.bev_buffer_mode
    call_strat.rally_base = temp.rally_base
    call_strat.minute_interval = temp.minute_interval
    call_strat.opt_momentum_threshold = temp.opt_momentum_threshold
    call_strat.require_lower_wick = temp.require_lower_wick
    call_strat.block_marubozu = temp.block_marubozu
    call_strat.max_range = temp.max_range
    call_strat.max_momentum = temp.max_momentum
    call_strat.max_above_high = temp.max_above_high
    call_strat.rebuy_drop = temp.rebuy_drop
    call_strat.max_hold = temp.max_hold
    call_strat.target_profit = temp.target_profit
    call_strat.first_only = temp.first_only
    call_strat.future_key = future_key

    put_strat = OptionFutureMomentum(side="PUT")
    put_strat.opt_low_margin = temp.opt_low_margin
    put_strat.fut_low_margin = temp.fut_low_margin
    put_strat.trail_tiers = list(temp.trail_tiers)
    put_strat.trail_mode = temp.trail_mode
    put_strat.breakeven_trigger_pct = temp.breakeven_trigger_pct
    put_strat.breakeven_buffer = temp.breakeven_buffer
    put_strat.straight_down_stop_pct = temp.straight_down_stop_pct
    put_strat.fut_move_limit = temp.fut_move_limit
    put_strat.opt_open_rally_pct = temp.opt_open_rally_pct
    put_strat.opt_atp_distance_pct = temp.opt_atp_distance_pct
    put_strat.opt_buy_sell_ratio = temp.opt_buy_sell_ratio
    put_strat.bev_buffer_mode = temp.bev_buffer_mode
    put_strat.rally_base = temp.rally_base
    put_strat.minute_interval = temp.minute_interval
    put_strat.opt_momentum_threshold = temp.opt_momentum_threshold
    put_strat.require_lower_wick = temp.require_lower_wick
    put_strat.block_marubozu = temp.block_marubozu
    put_strat.max_range = temp.max_range
    put_strat.max_momentum = temp.max_momentum
    put_strat.max_above_high = temp.max_above_high
    put_strat.rebuy_drop = temp.rebuy_drop
    put_strat.max_hold = temp.max_hold
    put_strat.target_profit = temp.target_profit
    put_strat.first_only = temp.first_only
    put_strat.future_key = future_key

    # Create executors (one per side)
    call_executor = create_executor(exec_mode, call_name, call_key, lot_size, max_trades,
                                     sandbox_token, kotak_client=kotak_client, sub_meta=sub_meta)
    put_executor = create_executor(exec_mode, put_name, put_key, lot_size, max_trades,
                                    sandbox_token, kotak_client=kotak_client, sub_meta=sub_meta)

    runner = ExecutionDualRunner(
        call_strat, call_executor, call_key,
        put_strat, put_executor, put_key,
        future_key
    )

    fut_display = sub_meta.get(future_key, {}).get("name", future_key) if future_key else "none"
    print(f"\n  ✓ Dual CE+PE Momentum [{exec_mode.value}]")
    print(f"    CE: {call_name:<28} {call_key}")
    print(f"    PE: {put_name:<28} {put_key}")
    print(f"    Future: {fut_display}")
    print(f"    Lot: {lot_size} │ Max trades per side: {max_trades}")
    call_strat.describe()
    put_strat.describe()

    return runner


# ═══════════════════════════════════════════════════════════════
# STREAM — reuse upstox_live.py's stream() function
# ═══════════════════════════════════════════════════════════════
# The stream() function from upstox_live.py works with any object that has:
#   - .on_tick(key, ltp, ts, feed_data)
#   - .instrument_key
#   - .future_key
#   - .trader.instrument_name, .trader.lot_size, .trader.max_trades
#   - .strategy.name
#   - .print_final_summary()
#
# Our ExecutionStrategyRunner and ExecutionDualRunner implement all of these.
# We import stream() directly from upstox_live.py — no changes needed.
from upstox_live import stream


def _update_executor_symbols(strategy_runner, sub_meta):
    """
    After resolve_kotak_symbols fills sub_meta with kotak_symbol,
    update any already-created KotakNeoExecutor instances.
    Sets the verified flag — executors will BLOCK orders if not verified.
    """
    executors = []
    if isinstance(strategy_runner, ExecutionDualRunner):
        executors = [
            (strategy_runner.call_key, strategy_runner.call_trader),
            (strategy_runner.put_key, strategy_runner.put_trader),
        ]
    elif isinstance(strategy_runner, ExecutionStrategyRunner):
        executors = [(strategy_runner.instrument_key, strategy_runner.trader)]

    updated = 0
    for inst_key, executor in executors:
        if isinstance(executor, KotakNeoExecutor):
            meta = sub_meta.get(inst_key, {})
            resolved = meta.get("kotak_symbol", "")
            is_verified = meta.get("kotak_verified", False)
            token_mismatch = meta.get("kotak_token_mismatch", False)

            if resolved and resolved != executor.trading_symbol:
                old = executor.trading_symbol
                executor.trading_symbol = resolved
                print(f"  ✓ Updated executor symbol: {old} → {resolved}")

            # Set verified flag
            executor.verified = is_verified
            if is_verified:
                print(f"  ✓ {executor.trading_symbol}: VERIFIED — orders enabled")
                updated += 1
            else:
                reason = "token mismatch" if token_mismatch else "search_scrip failed"
                print(f"  ✗ {executor.trading_symbol}: NOT VERIFIED ({reason}) — orders BLOCKED")

    if updated:
        print(f"  {updated} executor(s) verified and ready for live trading")


def _show_pretrade_confirmation(strategy_runner, sub_meta, exec_mode):
    """
    Show a clear summary of exactly what instruments will be traded via Kotak,
    with the full mapping chain for each. Returns True only if ALL checks pass.
    """
    if exec_mode != ExecMode.LIVE:
        return True

    print(f"\n  {'═'*65}")
    print(f"  ⚠  LIVE TRADING CONFIRMATION — REAL MONEY")
    print(f"  {'═'*65}")

    if not strategy_runner:
        print(f"\n  No strategy active. Streaming only — no orders will be placed.")
        return True

    # Collect the instruments that will actually receive orders
    trade_instruments = []
    if isinstance(strategy_runner, ExecutionDualRunner):
        trade_instruments.append(("CE (BUY side)", strategy_runner.call_key, strategy_runner.call_trader))
        trade_instruments.append(("PE (BUY side)", strategy_runner.put_key, strategy_runner.put_trader))
        if strategy_runner.future_key:
            fut_meta = sub_meta.get(strategy_runner.future_key, {})
            print(f"\n  Strategy: Dual CE+PE Momentum")
            print(f"  Future (data only, no orders): {fut_meta.get('name', '?')}")
    elif isinstance(strategy_runner, ExecutionStrategyRunner):
        trade_instruments.append(("TRADE", strategy_runner.instrument_key, strategy_runner.trader))

    print(f"\n  The strategy will place REAL BUY/SELL orders on these instruments:")
    print(f"  {'─'*65}")

    all_ok = True
    blockers = []

    for label, inst_key, executor in trade_instruments:
        meta = sub_meta.get(inst_key, {})
        upstox_name = meta.get("name", "?")
        kotak_sym = meta.get("kotak_symbol", "")
        kotak_token = meta.get("kotak_token", "")
        kotak_seg = meta.get("kotak_exchange_segment", "")
        kotak_verified = meta.get("kotak_verified", False)
        token_mismatch = meta.get("kotak_token_mismatch", False)
        upstox_ltp = meta.get("ltp", "?")
        upstox_token = inst_key.split("|")[-1] if "|" in inst_key else ""

        # What the executor will actually send to Kotak
        if isinstance(executor, KotakNeoExecutor):
            actual_sym = executor.trading_symbol
            actual_seg = executor.exchange_segment
            lot_size = executor.lot_size
            is_verified = executor.verified
        else:
            actual_sym = "(paper — no Kotak orders)"
            actual_seg = ""
            lot_size = executor.lot_size if hasattr(executor, 'lot_size') else "?"
            is_verified = True  # paper doesn't need verification

        # ── Run all safety checks ──
        checks = []

        # Check 1: Symbol prefix matches underlying
        underlying = upstox_name.split()[0].upper() if upstox_name else ""
        if kotak_sym and underlying:
            if kotak_sym.upper().startswith(underlying):
                checks.append(("✓", "Symbol prefix matches"))
            else:
                checks.append(("✗", f"MISMATCH: expected '{underlying}*' got '{kotak_sym}'"))
                all_ok = False
                blockers.append(f"{label}: symbol mismatch")

        # Check 2: Token cross-check (Upstox token = Kotak pSymbol)
        if kotak_token and upstox_token:
            if kotak_token == upstox_token:
                checks.append(("✓", f"Token match (both {kotak_token})"))
            else:
                checks.append(("✗", f"TOKEN MISMATCH: Upstox={upstox_token} Kotak={kotak_token}"))
                all_ok = False
                blockers.append(f"{label}: token mismatch")
        elif not kotak_token:
            checks.append(("✗", "No Kotak token (search_scrip failed)"))
            all_ok = False
            blockers.append(f"{label}: unresolved symbol")

        # Check 3: Verification status
        if is_verified:
            checks.append(("✓", "LTP verified via Kotak quotes"))
        else:
            checks.append(("✗", "NOT verified — orders will be BLOCKED"))
            all_ok = False
            blockers.append(f"{label}: not verified")

        # Check 4: Lot size sanity
        if isinstance(lot_size, int) and lot_size > 0:
            checks.append(("✓", f"Order qty = {lot_size} shares"))
        else:
            checks.append(("⚠", f"Lot size = {lot_size} — verify this is correct"))

        # Display
        print(f"\n  [{label}]")
        print(f"    Upstox:      {upstox_name:<30} key={inst_key}")
        print(f"    Kotak order: {actual_sym:<30} segment={actual_seg}")
        print(f"    Token:       Upstox={upstox_token:<10} Kotak={kotak_token or '?'}")
        print(f"    Order qty:   {lot_size} shares per order")
        if upstox_ltp and upstox_ltp != "?":
            cost = float(upstox_ltp) * (lot_size if isinstance(lot_size, int) else 1)
            print(f"    Approx cost: ₹{upstox_ltp} × {lot_size} = ₹{cost:,.0f} per trade")
        for status, msg in checks:
            print(f"    {status} {msg}")

    print(f"\n  {'─'*65}")
    if all_ok:
        print(f"  ✓ ALL CHECKS PASSED — safe to proceed")
    else:
        print(f"  ✗ SAFETY CHECKS FAILED:")
        for b in blockers:
            print(f"    ✗ {b}")
        print(f"\n  Even if you type 'GO', unverified executors will BLOCK orders.")
        print(f"  Orders will only execute on verified instruments.")

    return all_ok


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    # Reuse upstox_live's logging
    log_path = start_logging()

    print("\n" + "=" * 60)
    print("  STRATEGY EXECUTION ENGINE")
    print("  Data: Upstox │ Execution: Paper / Sandbox / Live")
    print("=" * 60)
    print(f"  Log: {log_path}")

    # ── Select execution mode ──
    # ── Auto-detect credentials from globals (set in Colab cell or at top of file) ──
    _creds = {
        "ACCESS_TOKEN": globals().get("ACCESS_TOKEN", ""),
        "sandbox_access_token": globals().get("sandbox_access_token", ""),
        "kotak_consumer_key": globals().get("kotak_consumer_key", "5871e03f-daef-4244-ae49-a155e9558358"),
        "kotak_mobile_number": globals().get("kotak_mobile_number", "+918095472479"),
        "kotak_ucc": globals().get("kotak_ucc", "X76BE"),
        "kotak_mpin": globals().get("kotak_mpin", "809547"),
    }
    _has_creds = bool(_creds["ACCESS_TOKEN"])
    _has_kotak = bool(_creds["kotak_consumer_key"])
    if _has_creds:
        print(f"  ✓ Credentials loaded from globals (ACCESS_TOKEN, kotak_*)")

    print(f"\n  Execution mode:")
    print(f"    1. Paper    — in-memory simulation (default)")
    print(f"    2. Sandbox  — Upstox sandbox API (real validation, fake money)")
    print(f"    3. Live     — Kotak Neo")
    mode_pick = input(f"\n  Pick mode [1]: ").strip()

    if mode_pick == "2":
        exec_mode = ExecMode.SANDBOX
        sandbox_token = _creds["sandbox_access_token"]
        if not sandbox_token:
            sandbox_token = input("  Paste your Upstox SANDBOX token: ").strip()
        if not sandbox_token:
            print("  No sandbox token. Falling back to paper mode.")
            exec_mode = ExecMode.PAPER
            sandbox_token = None
        else:
            print(f"  ✓ Sandbox token loaded")
    elif mode_pick == "3":
        exec_mode = ExecMode.LIVE
        sandbox_token = None
        kotak_client = _kotak_login(
            _creds["kotak_consumer_key"],
            _creds["kotak_mobile_number"],
            _creds["kotak_ucc"],
            _creds["kotak_mpin"],
        )
        if not kotak_client:
            print("  ⚠ Kotak login failed. Falling back to paper mode.")
            exec_mode = ExecMode.PAPER
    else:
        exec_mode = ExecMode.PAPER
        sandbox_token = None

    if exec_mode != ExecMode.LIVE:
        kotak_client = None

    mode_label = exec_mode.value.upper()
    print(f"\n  ✓ Execution mode: {mode_label}")

    # ── Data layer token (Upstox analytics — for streaming) ──
    data_token = _creds["ACCESS_TOKEN"]
    if not data_token:
        data_token = input("\n  Paste your Upstox ANALYTICS token (for data): ").strip()
    else:
        print(f"  ✓ Upstox analytics token loaded from globals")
    if not data_token:
        print("  No token. Exiting.")
        return

    MODE = "full_d30"
    client = make_client(data_token)
    result = show_account_info(data_token)
    if result == "downgrade":
        MODE = "full"

    print("\n  Validating data token...")
    validate_token(client)

    # ── Subscription menu (same as upstox_live.py) ──
    sub_keys = []
    sub_meta = {}
    strategy_runner = None

    while True:
        n = len(sub_keys)
        strat_status = f" │ Strategy: {strategy_runner.strategy.name}" if strategy_runner else ""
        print(f"\n  ── [{mode_label}] Subscription List ({n} instruments{strat_status}) ──")

        if sub_keys:
            for i, k in enumerate(sub_keys, 1):
                meta = sub_meta.get(k, {})
                name = meta.get("name", k.split("|")[-1])
                ltp = meta.get("ltp")
                ltp_str = f"₹{ltp}" if ltp else ""
                kotak_sym = meta.get("kotak_symbol", "")
                kotak_str = f"  [{kotak_sym}]" if kotak_sym and exec_mode == ExecMode.LIVE else ""
                print(f"    {i}. {name:<28} {ltp_str:>10}   {k}{kotak_str}")
        else:
            print("    (empty)")

        print(f"\n  What to add?")
        print(f"    1. Index options  (chain picker)")
        print(f"    2. Index itself   (Nifty, BankNifty, Sensex, VIX)")
        print(f"    3. Index futures  (e.g. Nifty FUT)")
        print(f"    4. Stock          (search by name)")
        print(f"    5. Custom key     (paste instrument key)")
        print(f"    6. MCX commodity  (Crude, Gold, Silver — till 11:30 PM)")
        print(f"    7. Currency       (USDINR, EURINR — till 5:00 PM)")
        print(f"    ──────────────────")
        print(f"    8. Remove items")
        print(f"    9. Change mode    (current: {MODE})")
        print(f"   10. Strategy       {'✓ ' + strategy_runner.strategy.name if strategy_runner else '(none)'}")
        print(f"   11. Switch exec    (current: {mode_label})")
        print(f"   12. Re-enter data token")
        print(f"    ──────────────────")
        print(f"    0. START STREAMING")
        print(f"    q. Quit")

        choice = input(f"\n  > ").strip()

        if choice == "1":
            add_index_options(client, sub_keys, sub_meta)
        elif choice == "2":
            add_index_for_streaming(client, sub_keys, sub_meta)
        elif choice == "3":
            add_futures(client, sub_keys, sub_meta)
        elif choice == "4":
            add_stock(client, sub_keys, sub_meta)
        elif choice == "5":
            add_custom_key(client, sub_keys, sub_meta)
        elif choice == "6":
            add_mcx_commodity(client, sub_keys, sub_meta)
        elif choice == "7":
            add_currency(client, sub_keys, sub_meta)
        elif choice == "8":
            if not sub_keys:
                print("  Nothing to remove.")
                continue
            pick = input("  Remove # (or 'clear'): ").strip()
            if pick.lower() == "clear":
                sub_keys.clear()
                sub_meta.clear()
                strategy_runner = None
                print("  Cleared all.")
            else:
                try:
                    idx = int(pick) - 1
                    removed = sub_keys.pop(idx)
                    rname = sub_meta.pop(removed, {}).get("name", removed)
                    if strategy_runner and strategy_runner.instrument_key == removed:
                        strategy_runner = None
                        print(f"  Removed: {rname} (strategy cleared)")
                    else:
                        print(f"  Removed: {rname}")
                except (ValueError, IndexError):
                    print("  Invalid.")
        elif choice == "9":
            print(f"  Modes: full, full_d30, ltpc, option_greeks")
            new_mode = input(f"  New mode [{MODE}]: ").strip() or MODE
            if new_mode in ("full", "full_d30", "ltpc", "option_greeks"):
                MODE = new_mode
                print(f"  ✓ Mode set to '{MODE}'")
            else:
                print(f"  Invalid mode.")
        elif choice == "10":
            if not sub_keys:
                print("  Add instruments first, then set strategy.")
                continue
            strategy_runner = setup_execution_strategy(
                sub_keys, sub_meta, exec_mode, sandbox_token,
                kotak_client=kotak_client if exec_mode == ExecMode.LIVE else None,
                client=client,
            )
        elif choice == "11":
            print(f"  Current: {mode_label}")
            print(f"    1. Paper  2. Sandbox  3. Live")
            sw = input(f"  Switch to: ").strip()
            if sw == "1":
                exec_mode = ExecMode.PAPER
                sandbox_token = None
                kotak_client = None
            elif sw == "2":
                exec_mode = ExecMode.SANDBOX
                sandbox_token = input("  Sandbox token: ").strip()
                if not sandbox_token:
                    exec_mode = ExecMode.PAPER
                kotak_client = None
            elif sw == "3":
                # Login to Kotak using saved creds
                kotak_client = _kotak_login(
                    _creds["kotak_consumer_key"],
                    _creds["kotak_mobile_number"],
                    _creds["kotak_ucc"],
                    _creds["kotak_mpin"],
                )
                if kotak_client:
                    exec_mode = ExecMode.LIVE
                else:
                    print("  ⚠ Kotak login failed. Staying in current mode.")
                    continue
            mode_label = exec_mode.value.upper()
            strategy_runner = None
            print(f"  ✓ Switched to {mode_label}. Re-configure strategy (option 10).")
        elif choice == "12":
            new_token = input("  Paste new Upstox ANALYTICS token: ").strip()
            if new_token:
                data_token = new_token
                client = make_client(data_token)
                result = show_account_info(data_token)
                if result == "downgrade":
                    MODE = "full"
                print("  Validating new token...")
                validate_token(client)
                sub_keys.clear()
                sub_meta.clear()
                strategy_runner = None
                print("  ✓ Token updated. Subscription list cleared — re-add instruments.")
        elif choice == "0":
            if not sub_keys:
                print("  Add at least one instrument first!")
                continue
            seen = set()
            unique = [k for k in sub_keys if not (k in seen or seen.add(k))]
            sub_keys = unique
            limit = {"full_d30": 50, "full": 2000, "ltpc": 5000, "option_greeks": 3000}.get(MODE, 2000)
            if len(sub_keys) > limit:
                print(f"  ⚠ '{MODE}' allows max {limit}. Truncating.")
                sub_keys = sub_keys[:limit]

            # Resolve Kotak symbols before streaming (live mode only)
            if exec_mode == ExecMode.LIVE and kotak_client:
                resolve_kotak_symbols(kotak_client, sub_keys, sub_meta)

                # Update executors that were created before resolution
                if strategy_runner:
                    _update_executor_symbols(strategy_runner, sub_meta)

                # ── PRE-TRADE CONFIRMATION ──
                all_safe = _show_pretrade_confirmation(strategy_runner, sub_meta, exec_mode)

                if all_safe:
                    confirm = input("\n  Type 'GO' to start live trading, anything else to abort: ").strip()
                    if confirm != "GO":
                        print("  Aborted. Returning to menu.")
                        continue
                else:
                    print(f"\n  Safety checks FAILED. Unverified instruments will BLOCK orders.")
                    print(f"  You can still stream (verified instruments will trade, others won't).")
                    confirm = input("  Type 'FORCE' to proceed anyway, anything else to abort: ").strip()
                    if confirm != "FORCE":
                        print("  Aborted. Fix the issues and try again.")
                        continue
                    print("  Proceeding with FORCE — unverified executors will block orders at runtime.")

            stream(client, sub_keys, MODE, sub_meta, strategy_runner=strategy_runner)
            break
        elif choice.lower() == "q":
            print("  Bye!")
            return
        else:
            print("  Invalid choice.")


if __name__ == "__main__":
    try:
        main()
    finally:
        stop_logging()
