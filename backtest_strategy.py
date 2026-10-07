import time
"""
Backtest: Dual CE+PE Momentum Strategy on Harvested Data
=========================================================
Replays saved JSONL tick data through the same strategy engine
used in upstox_live.py — no code duplication.

Usage:
  python backtest_strategy.py                         # auto-scan all dates
  python backtest_strategy.py --date 2026-08-17       # single date
  python backtest_strategy.py --data-dir /path/to/live_data
  python backtest_strategy.py --list                  # list available data

Data format (from auto_harvest / upstox_live):
  Each line: {"ts": "...", "key": "NSE_FO|12345", "data": { ... }}
"""

import json
import os
import sys
import re
import glob
from datetime import datetime, timezone, timedelta
from collections import defaultdict

IST = timezone(timedelta(hours=5, minutes=30))

# ═══════════════════════════════════════════════════════════════
# TEE LOGGER — mirrors all output to a timestamped file
# ═══════════════════════════════════════════════════════════════

LOG_DIR = "backtest_logs"

class TeeLogger:
    """Writes to both terminal and a log file simultaneously."""

    def __init__(self, log_path):
        self.terminal = sys.stdout
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        self.log_file = open(log_path, "a", buffering=1, encoding="utf-8")
        self.log_path = log_path

    def write(self, message):
        self.terminal.write(message)
        if message.strip():
            import datetime
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            self.log_file.write(f"[{ts}] {message}")
        else:
            self.log_file.write(message)

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

    def close(self):
        self.log_file.close()

    def isatty(self):
        return self.terminal.isatty()

    @property
    def encoding(self):
        return self.terminal.encoding


def start_logging():
    now = datetime.now(IST)
    filename = f"backtest_{now.strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_path = os.path.join(LOG_DIR, filename)
    tee = TeeLogger(log_path)
    sys.stdout = tee
    sys.stderr = tee
    return log_path


def stop_logging():
    if isinstance(sys.stdout, TeeLogger):
        sys.stdout.close()
        sys.stdout = sys.stdout.terminal
    if isinstance(sys.stderr, TeeLogger):
        sys.stderr.close()
        sys.stderr = sys.stderr.terminal

# ═══════════════════════════════════════════════════════════════
# IMPORT STRATEGY ENGINE from upstox_live.py
# ═══════════════════════════════════════════════════════════════

# We import the core classes directly to stay in sync
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Inline the minimal strategy engine to avoid import issues with upstox SDK
# These are the same classes from upstox_live.py, self-contained for backtest

def extract_field(feed_data, field):
    """Extract a field from nested Upstox feed data."""
    if not isinstance(feed_data, dict):
        return None
    if feed_data.get(field) is not None:
        return feed_data[field]
    for ff_key in ("ff", "fullFeed", "full_feed"):
        ff = feed_data.get(ff_key) or {}
        for mff_key in ("marketFF", "indexFF", "market_ff", "index_ff"):
            mff = ff.get(mff_key) or {}
            if mff.get(field) is not None:
                return mff[field]
    for v in feed_data.values():
        if isinstance(v, dict):
            if v.get(field) is not None:
                return v[field]
            for v2 in v.values():
                if isinstance(v2, dict) and v2.get(field) is not None:
                    return v2[field]
    return None


def extract_ohlc(feed_data):
    """Extract daily OHLC from feed data. Handles marketOHLC.ohlc[] array."""
    result = {"open": None, "high": None, "low": None, "close": None}
    if not isinstance(feed_data, dict):
        return result
    for ff_key in ("ff", "fullFeed", "full_feed"):
        ff = feed_data.get(ff_key) or {}
        for mff_key in ("marketFF", "indexFF", "market_ff", "index_ff"):
            mff = ff.get(mff_key) or {}
            market_ohlc = mff.get("marketOHLC") or mff.get("market_ohlc") or {}
            ohlc_list = market_ohlc.get("ohlc") or []
            if isinstance(ohlc_list, list):
                for entry in ohlc_list:
                    if not isinstance(entry, dict):
                        continue
                    interval = entry.get("interval") or entry.get("intv") or ""
                    if interval in ("1d", "1D"):
                        for k in ("open", "high", "low", "close"):
                            if entry.get(k) is not None:
                                result[k] = entry[k]
                        return result
                if ohlc_list and isinstance(ohlc_list[0], dict):
                    for k in ("open", "high", "low", "close"):
                        if ohlc_list[0].get(k) is not None:
                            result[k] = ohlc_list[0][k]
                    if result["low"] is not None:
                        return result
            ohlc = mff.get("ohlc") or {}
            if isinstance(ohlc, dict) and ohlc:
                for k in ("open", "high", "low", "close"):
                    if ohlc.get(k) is not None:
                        result[k] = ohlc[k]
                if result["low"] is not None:
                    return result
    return result


def extract_ltp(feed_data):
    """Extract LTP from feed data — matches live upstox_live.py exactly."""
    if not isinstance(feed_data, dict):
        return None
    for ff_key in ("ff", "fullFeed", "full_feed"):
        ff = feed_data.get(ff_key) or {}
        for mff_key in ("marketFF", "indexFF", "market_ff", "index_ff", "marketFf", "indexFf"):
            ltpc = (ff.get(mff_key) or {}).get("ltpc") or {}
            if ltpc.get("ltp") is not None:
                return ltpc["ltp"]
    for flwg_key in ("firstLevelWithGreeks", "first_level_with_greeks"):
        ltpc = (feed_data.get(flwg_key) or {}).get("ltpc") or {}
        if ltpc.get("ltp") is not None:
            return ltpc["ltp"]
    ltpc = feed_data.get("ltpc") or {}
    if ltpc.get("ltp") is not None:
        return ltpc["ltp"]
    return feed_data.get("ltp")


def extract_close(feed_data):
    """Extract previous day close (cp) from feed data — matches live exactly."""
    if not isinstance(feed_data, dict):
        return None
    for ff_key in ("ff", "fullFeed", "full_feed"):
        ff = feed_data.get(ff_key) or {}
        for mff_key in ("marketFF", "indexFF", "market_ff", "index_ff"):
            ltpc = (ff.get(mff_key) or {}).get("ltpc") or {}
            if ltpc.get("cp") is not None:
                return ltpc["cp"]
    ltpc = feed_data.get("ltpc") or {}
    if ltpc.get("cp") is not None:
        return ltpc["cp"]
    return None


def extract_market_data(feed_data):
    """Extract ATP, TBQ, TSQ, OI, IV from feed data."""
    result = {"atp": None, "tbq": None, "tsq": None, "oi": None, "iv": None}
    if not isinstance(feed_data, dict):
        return result
    for ff_key in ("ff", "fullFeed", "full_feed"):
        ff = feed_data.get(ff_key) or {}
        for mff_key in ("marketFF", "indexFF", "market_ff", "index_ff"):
            mff = ff.get(mff_key) or {}
            for field in result:
                if result[field] is None and mff.get(field) is not None:
                    result[field] = mff[field]
    return result


def extract_greek(feed_data, greek_name):
    """Extract a specific greek (delta, theta, gamma, vega, iv) from feed data."""
    if not isinstance(feed_data, dict):
        return None
    for ff_key in ("ff", "fullFeed", "full_feed"):
        ff = feed_data.get(ff_key) or {}
        for mff_key in ("marketFF", "indexFF", "market_ff", "index_ff"):
            mff = ff.get(mff_key) or {}
            greeks = mff.get("optionGreeks") or mff.get("option_greeks") or {}
            if greeks.get(greek_name) is not None:
                return greeks[greek_name]
    for flwg_key in ("firstLevelWithGreeks", "first_level_with_greeks"):
        flwg = feed_data.get(flwg_key) or {}
        greeks = flwg.get("optionGreeks") or flwg.get("option_greeks") or {}
        if greeks.get(greek_name) is not None:
            return greeks[greek_name]
    return None


class PaperTrader:
    """Paper trading engine — tracks buy/sell/P&L."""
    def __init__(self, name, lot_size=1, max_trades=999):
        self.instrument_name = name
        self.lot_size = lot_size
        self.max_trades = max_trades
        self.in_position = False
        self.entry_price = None
        self.entry_time = None
        self.current_ltp = None
        self.trades = []  # completed trades

    def can_buy(self):
        return not self.in_position and len(self.trades) < self.max_trades

    def can_sell(self):
        return self.in_position

    def buy(self, price, timestamp, prevmin_ohlc=None, entry_greeks=None, entry_fut=None):
        if not self.can_buy():
            return False
        self.in_position = True
        self.entry_price = price
        self.entry_time = timestamp
        self.peak_price = price
        self.prevmin_ohlc = prevmin_ohlc  # store for trade details
        self.entry_greeks = entry_greeks
        self.entry_fut = entry_fut
        return True

    def sell(self, price, timestamp, peak=None, bev=False, wide=False, trail=False):
        if not self.can_sell():
            return False
        pnl = (price - self.entry_price) * self.lot_size
        pct = ((price - self.entry_price) / self.entry_price) * 100
        self.trades.append({
            "entry_price": self.entry_price,
            "exit_price": price,
            "peak_price": peak or self.peak_price or price,
            "entry_time": self.entry_time,
            "exit_time": timestamp,
            "pnl": round(pnl, 2),
            "pct": round(pct, 1),
            "bev": bev,
            "wide": wide,
            "trail": trail,
            "prevmin": self.prevmin_ohlc,
            "greeks": getattr(self, "entry_greeks", None),
            "entry_fut": getattr(self, "entry_fut", None),
        })
        self.in_position = False
        self.entry_price = None
        self.entry_time = None
        self.peak_price = None
        return True

    def update_tick(self, ltp, timestamp):
        self.current_ltp = ltp

    def unrealized_pnl(self):
        if not self.in_position or not self.current_ltp:
            return 0, 0
        pnl = (self.current_ltp - self.entry_price) * self.lot_size
        pct = ((self.current_ltp - self.entry_price) / self.entry_price) * 100
        return round(pnl, 2), round(pct, 1)


class OptionFutureMomentum:
    """
    Strategy: Option + Future Momentum with Tiered Trailing Stop + BEV + Two-Tier Stop.
    Matches the live upstox_live.py strategy exactly.
    """
    def __init__(self, side="CALL", opt_low_margin=5, fut_low_margin=10, trail_tiers=None,
                 bev_trigger=2.0, bev_buffer=1.0, bev_buffer_mode="rupees", straight_down_pct=25.0, fut_move_limit=-1,
                 opt_open_rally_pct=50, opt_atp_distance_pct=30, opt_buy_sell_ratio=0.0,
                 rally_base="daylow_prevclose", minute_interval=1,
                 opt_momentum_threshold=1.0):
        self.side = side.upper()
        self.name = f"Option+Fut Momentum ({self.side})"
        self.minute_interval = minute_interval  # check every N minutes
        self.opt_momentum_threshold = opt_momentum_threshold  # LTP must exceed prev_avg by this much
        self.opt_low_margin = opt_low_margin
        self.fut_low_margin = fut_low_margin
        self.trail_mode = "pct"
        self.trail_tiers = trail_tiers or [(0, 1), (2, 1), (6, 2), (15, 5)]
        # ── Breakeven lock ──
        self.breakeven_trigger_pct = bev_trigger  # 0 = activate on ANY increase
        self.breakeven_buffer = bev_buffer
        self.bev_buffer_mode = bev_buffer_mode  # "rupees" or "pct"
        self._breakeven_active = False
        self._breakeven_stop = None
        # ── Two-tier stop ──
        self.straight_down_stop_pct = straight_down_pct
        # ── Future-from-close filter ──
        self.fut_move_limit = fut_move_limit
        self.fut_prev_close = None
        # ── Entry filters: avoid buying at the high ──
        self.opt_day_open = None
        self.opt_prev_day_close = None    # previous day close of option
        self.opt_atp = None
        self.opt_tbq = None
        self.opt_tsq = None
        self.opt_open_rally_pct = opt_open_rally_pct
        self.opt_atp_distance_pct = opt_atp_distance_pct
        self.opt_buy_sell_ratio = opt_buy_sell_ratio
        self.rally_base = rally_base  # "open" or "daylow_prevclose"
        self._verbose = False  # set to True for detailed condition printing
        # ── Minute-level tracking ──
        self.tick_count = 0
        self._current_minute = None
        self._min_opt_ltps = []
        self._min_opt_vols = []
        self._min_fut_ltps = []
        self.opt_prev_avg_ltp = None
        self.opt_prev_avg_vol = None
        self.fut_prev_avg_ltp = None
        # Previous minute OHLC (for analysis)
        self.opt_prev_min_open = None
        self.opt_prev_min_high = None
        self.opt_prev_min_low = None
        self.opt_prev_min_close = None
        self.opt_day_low = None
        self.fut_day_low = None
        self.fut_day_high = None
        self.opt_ltp = None
        self.opt_vol = None
        self.fut_ltp = None
        self._in_position = False
        self.entry_price = None
        self.peak_price = None
        self.stop_loss = None
        self._sell_minute = None
        self._buy_check_pending = False
        self._minutes_seen = 0
        self._interval_counter = 0  # counts minute boundaries within current interval
        # ── Last exit info (for backtest to read after SELL) ──
        self.last_exit_bev = False
        self.last_exit_wide = False
        self.last_exit_trail = False
        self.last_exit_peak = None

    def _print_verbose(self, timestamp, checks, result):
        """Print detailed condition check for this minute."""
        ts = timestamp[11:19] if len(timestamp) > 18 else timestamp[-8:]
        minute = timestamp[11:16] if len(timestamp) > 15 else "?"
        print(f"    [{self.side} {minute}] {result}")
        for c in checks:
            print(f"      {c}")
        print()

    def _get_trail_distance(self, gain_pct):
        trail_val = self.trail_tiers[0][1]
        for threshold, tv in self.trail_tiers:
            if gain_pct >= threshold:
                trail_val = tv
        if self.trail_mode == "pct":
            return self.entry_price * (trail_val / 100)
        else:
            return trail_val

    def _roll_minute(self, timestamp):
        if isinstance(timestamp, str):
            minute_key = timestamp[:16]
        else:
            minute_key = str(timestamp)[:16]

        if self._current_minute is None:
            self._current_minute = minute_key
            return

        if minute_key != self._current_minute:
            self._current_minute = minute_key
            self._interval_counter += 1

            if self._interval_counter < self.minute_interval:
                # Still accumulating ticks — don't compute averages yet
                return

            # N minutes reached — compute averages from all accumulated ticks
            if self._min_opt_ltps:
                self.opt_prev_avg_ltp = sum(self._min_opt_ltps) / len(self._min_opt_ltps)
                self.opt_prev_min_open = self._min_opt_ltps[0]
                self.opt_prev_min_high = max(self._min_opt_ltps)
                self.opt_prev_min_low = min(self._min_opt_ltps)
                self.opt_prev_min_close = self._min_opt_ltps[-1]
            if self._min_opt_vols:
                self.opt_prev_avg_vol = sum(self._min_opt_vols) / len(self._min_opt_vols)
            if self._min_fut_ltps:
                self.fut_prev_avg_ltp = sum(self._min_fut_ltps) / len(self._min_fut_ltps)
            self._min_opt_ltps = []
            self._min_opt_vols = []
            self._min_fut_ltps = []
            self._interval_counter = 0
            self._minutes_seen += 1
            if self._minutes_seen >= 1 and self.opt_prev_avg_ltp is not None and self.fut_prev_avg_ltp is not None:
                self._buy_check_pending = True

    def on_option_tick(self, ltp, timestamp, feed_data=None):
        self.tick_count += 1
        ltp = float(ltp)
        self.opt_ltp = ltp

        vol = extract_field(feed_data, "v") or extract_field(feed_data, "vtt") if feed_data else None
        if vol is not None:
            try:
                self.opt_vol = float(vol)
            except (ValueError, TypeError):
                pass

        # Day low from OHLC (real market) or fallback to tick tracking
        if self.opt_day_low is None or ltp < self.opt_day_low:
            self.opt_day_low = ltp
        if feed_data:
            ohlc = extract_ohlc(feed_data)
            if ohlc["low"] is not None and ohlc["low"] > 0:
                self.opt_day_low = ohlc["low"]
            if self.opt_day_open is None and ohlc["open"] is not None and ohlc["open"] > 0:
                self.opt_day_open = ohlc["open"]
            # Capture option's previous day close (from cp field)
            if self.opt_prev_day_close is None and feed_data:
                cp = extract_close(feed_data)
                if cp is not None and cp > 0:
                    self.opt_prev_day_close = cp
            mkt = extract_market_data(feed_data)
            if mkt["atp"] is not None and mkt["atp"] > 0:
                self.opt_atp = mkt["atp"]
            if mkt["tbq"] is not None:
                self.opt_tbq = mkt["tbq"]
            if mkt["tsq"] is not None:
                self.opt_tsq = mkt["tsq"]

        self._roll_minute(timestamp)
        self._min_opt_ltps.append(ltp)
        if self.opt_vol is not None:
            self._min_opt_vols.append(float(self.opt_vol))

        # ── In position: two-tier stop management ──
        if self._in_position:
            if ltp > self.peak_price:
                self.peak_price = ltp

            # Two-tier: wide stop until BEV can activate, then trail + BEV
            bev_threshold = self.entry_price * (self.breakeven_trigger_pct / 100)
            price_went_up = self.peak_price >= self.entry_price + bev_threshold

            if not price_went_up:
                # STRAIGHT DOWN: wider stop
                self.stop_loss = self.entry_price * (1 - self.straight_down_stop_pct / 100)
            else:
                # NORMAL: tiered trail + BEV
                gain = ((self.peak_price - self.entry_price) / self.entry_price) * 100
                trail = self._get_trail_distance(gain)
                trail_stop = self.peak_price - trail

                if not self._breakeven_active:
                    peak_gain_pct = ((self.peak_price - self.entry_price) / self.entry_price) * 100
                    if peak_gain_pct >= self.breakeven_trigger_pct - 0.001:  # tolerance for floating point
                        self._breakeven_active = True
                        if self.bev_buffer_mode == "pct":
                            self._breakeven_stop = self.entry_price * (1 + self.breakeven_buffer / 100)
                        else:
                            self._breakeven_stop = self.entry_price + self.breakeven_buffer

                if self._breakeven_active and self._breakeven_stop is not None:
                    self.stop_loss = max(trail_stop, self._breakeven_stop)
                else:
                    self.stop_loss = trail_stop

            if ltp <= self.stop_loss:
                # Save exit info BEFORE resetting (for backtest to read)
                self.last_exit_bev = self._breakeven_active and self._breakeven_stop is not None and self.stop_loss <= self._breakeven_stop + 0.01
                self.last_exit_wide = not price_went_up
                self.last_exit_trail = self._breakeven_active and not self.last_exit_bev
                self.last_exit_peak = self.peak_price
                self._in_position = False
                self.entry_price = None
                self.peak_price = None
                self.stop_loss = None
                self._breakeven_active = False
                self._breakeven_stop = None
                self._sell_minute = self._current_minute
                self._interval_counter = 0  # wait N more minutes before next check
                return "SELL"
            return None

        # ── Not in position: minute-gated entry check ──
        if not self._buy_check_pending:
            return None
        self._buy_check_pending = False

        if self._sell_minute is not None:
            if self._current_minute == self._sell_minute:
                return None
            self._sell_minute = None

        if self.opt_prev_avg_ltp is None or self.fut_prev_avg_ltp is None:
            return None
        if self.opt_prev_avg_vol is None:
            return None

        # Collect condition results for verbose output
        _v = getattr(self, '_verbose', False)
        checks = []

        c1 = ltp > self.opt_prev_avg_ltp + self.opt_momentum_threshold
        if _v: checks.append(f"{'✓' if c1 else '✗'} OptLTP {ltp:.2f} > prev_avg {self.opt_prev_avg_ltp:.2f} + {self.opt_momentum_threshold}")
        if not c1: 
            if _v: self._print_verbose(timestamp, checks, "FAIL: opt <= prev_avg")
            return None

        c2 = ltp > (self.opt_day_low or 0) + self.opt_low_margin
        if _v: checks.append(f"{'✓' if c2 else '✗'} OptLTP {ltp:.2f} > day_low {self.opt_day_low or 0:.2f} + {self.opt_low_margin}")
        if not c2:
            if _v: self._print_verbose(timestamp, checks, "FAIL: opt <= day_low+margin")
            return None

        c3 = not (self.opt_vol is not None and self.opt_vol <= self.opt_prev_avg_vol)
        if _v: checks.append(f"{'✓' if c3 else '✗'} Vol {self.opt_vol or 0:,.0f} > prev_vol {self.opt_prev_avg_vol:,.0f}")
        if not c3:
            if _v: self._print_verbose(timestamp, checks, "FAIL: vol <= prev_avg_vol")
            return None

        if self.fut_ltp is None:
            return None

        if self.side == "CALL":
            c4 = self.fut_ltp > self.fut_prev_avg_ltp
            if _v: checks.append(f"{'✓' if c4 else '✗'} FutLTP {self.fut_ltp:.2f} > fut_prev {self.fut_prev_avg_ltp:.2f}")
            if not c4:
                if _v: self._print_verbose(timestamp, checks, "FAIL: fut not rising")
                return None
            c5 = self.fut_ltp > (self.fut_day_low or 0) + self.fut_low_margin
            if _v: checks.append(f"{'✓' if c5 else '✗'} FutLTP {self.fut_ltp:.2f} > fut_day_low {self.fut_day_low or 0:.2f} + {self.fut_low_margin}")
            if not c5:
                if _v: self._print_verbose(timestamp, checks, "FAIL: fut at day low")
                return None
        else:  # PUT
            c4 = self.fut_ltp < self.fut_prev_avg_ltp
            if _v: checks.append(f"{'✓' if c4 else '✗'} FutLTP {self.fut_ltp:.2f} < fut_prev {self.fut_prev_avg_ltp:.2f}")
            if not c4:
                if _v: self._print_verbose(timestamp, checks, "FAIL: fut not falling")
                return None
            if self.fut_day_high is None:
                return None
            c5 = self.fut_ltp < self.fut_day_high - self.fut_low_margin
            if _v: checks.append(f"{'✓' if c5 else '✗'} FutLTP {self.fut_ltp:.2f} < fut_day_high {self.fut_day_high:.2f} - {self.fut_low_margin}")
            if not c5:
                if _v: self._print_verbose(timestamp, checks, "FAIL: fut at day high")
                return None

        # Future-from-close filter
        if self.fut_prev_close is not None and self.fut_move_limit > 0:
            fut_move = self.fut_ltp - self.fut_prev_close
            if self.side == "CALL" and fut_move > self.fut_move_limit:
                if _v: checks.append(f"✗ FutFromClose {fut_move:+.0f} > limit {self.fut_move_limit}")
                if _v: self._print_verbose(timestamp, checks, "FAIL: fut moved too far")
                return None
            if self.side == "PUT" and fut_move < -self.fut_move_limit:
                if _v: checks.append(f"✗ FutFromClose {fut_move:+.0f} < limit -{self.fut_move_limit}")
                if _v: self._print_verbose(timestamp, checks, "FAIL: fut moved too far")
                return None
            if _v: checks.append(f"✓ FutFromClose {fut_move:+.0f} within ±{self.fut_move_limit}")

        # Option rally filter
        if self.opt_open_rally_pct > 0:
            rally_base_price = None
            if self.rally_base == "open":
                if self.opt_day_open is not None and self.opt_day_open > 0:
                    rally_base_price = self.opt_day_open
            else:
                candidates = []
                if self.opt_prev_day_close is not None and self.opt_prev_day_close > 0:
                    candidates.append(self.opt_prev_day_close)
                if self.opt_day_low is not None and self.opt_day_low > 0:
                    candidates.append(self.opt_day_low)
                if candidates:
                    rally_base_price = min(candidates)
            if rally_base_price is not None and rally_base_price > 0:
                rally = (ltp - rally_base_price) / rally_base_price * 100
                if _v: checks.append(f"{'✗' if rally > self.opt_open_rally_pct else '✓'} Rally {rally:.1f}% vs limit {self.opt_open_rally_pct}% (base={rally_base_price:.2f} mode={self.rally_base})")
                if rally > self.opt_open_rally_pct:
                    if _v: self._print_verbose(timestamp, checks, "FAIL: rallied too much")
                    return None

        # ATP filter
        if self.opt_atp is not None and self.opt_atp > 0 and self.opt_atp_distance_pct > 0:
            atp_dist = (ltp - self.opt_atp) / self.opt_atp * 100
            if _v: checks.append(f"{'✗' if atp_dist > self.opt_atp_distance_pct else '✓'} ATP dist {atp_dist:.1f}% vs limit {self.opt_atp_distance_pct}% (ATP={self.opt_atp:.2f})")
            if atp_dist > self.opt_atp_distance_pct:
                if _v: self._print_verbose(timestamp, checks, "FAIL: too far above ATP")
                return None

        # Buy/Sell queue ratio
        if self.opt_buy_sell_ratio > 0 and self.opt_tsq is not None and self.opt_tsq > 0:
            ratio = (self.opt_tbq or 0) / self.opt_tsq
            if _v: checks.append(f"{'✗' if ratio < self.opt_buy_sell_ratio else '✓'} B/S ratio {ratio:.2f} vs min {self.opt_buy_sell_ratio}")
            if ratio < self.opt_buy_sell_ratio:
                if _v: self._print_verbose(timestamp, checks, "FAIL: sellers dominate")
                return None

        # ALL conditions met → BUY
        if _v:
            checks.append("✓✓✓ ALL CONDITIONS PASSED → BUY")
            if self.opt_prev_min_open is not None:
                rng = self.opt_prev_min_high - self.opt_prev_min_low
                checks.append(f"    PrevMin: O={self.opt_prev_min_open:.2f} H={self.opt_prev_min_high:.2f} L={self.opt_prev_min_low:.2f} C={self.opt_prev_min_close:.2f} Avg={self.opt_prev_avg_ltp:.2f} Range={rng:.2f}")
            self._print_verbose(timestamp, checks, "BUY")
        self._in_position = True
        self.entry_price = ltp
        self.peak_price = ltp
        self._breakeven_active = False
        self._breakeven_stop = None
        trail = self._get_trail_distance(0)
        self.stop_loss = ltp - trail
        return "BUY"

    def on_future_tick(self, ltp, timestamp, feed_data=None):
        ltp = float(ltp)
        self.fut_ltp = ltp
        if self.fut_day_low is None or ltp < self.fut_day_low:
            self.fut_day_low = ltp
        if self.fut_day_high is None or ltp > self.fut_day_high:
            self.fut_day_high = ltp
        if feed_data:
            ohlc = extract_ohlc(feed_data)
            if ohlc["low"] is not None and ohlc["low"] > 0:
                self.fut_day_low = ohlc["low"]
            if ohlc["high"] is not None and ohlc["high"] > 0:
                self.fut_day_high = ohlc["high"]
            # Capture previous day close (cp) from feed — only need it once
            if self.fut_prev_close is None:
                cp = extract_close(feed_data)
                if cp is not None and cp > 0:
                    self.fut_prev_close = float(cp)
        self._roll_minute(timestamp)
        self._min_fut_ltps.append(ltp)


# ═══════════════════════════════════════════════════════════════
# DATA LOADER — reads harvested JSONL files
# ═══════════════════════════════════════════════════════════════

def find_data_dirs(base_dir="live_data"):
    """Find all date directories with JSONL data.
    Supports both flat layout (live_data/DATE/*.jsonl) and
    subfolder layout (harvest_data/DATE/BANKNIFTY/*.jsonl)."""
    if not os.path.isdir(base_dir):
        return []
    dates = []
    for d in sorted(os.listdir(base_dir)):
        full = os.path.join(base_dir, d)
        if os.path.isdir(full) and re.match(r"\d{4}-\d{2}-\d{2}", d):
            # Count JSONL files in root AND subdirectories
            jsonl_files = glob.glob(os.path.join(full, "*.jsonl"))
            jsonl_files += glob.glob(os.path.join(full, "*", "*.jsonl"))
            jsonl_files += glob.glob(os.path.join(full, "*.parquet"))
            jsonl_files += glob.glob(os.path.join(full, "*", "*.parquet"))
            # Exclude _market_info and _metadata
            jsonl_files = [f for f in jsonl_files if not os.path.basename(f).startswith("_")]
            if jsonl_files:
                dates.append((d, full, len(jsonl_files)))
    return dates


def scan_instruments(day_dir):
    """Scan a day's directory and categorize instruments into CE/PE/FUT/INDEX.
    Handles both flat layout and subfolder layout (BANKNIFTY/, NIFTY/, SENSEX/)."""
    
    # Fast path: use _metadata.json if available (avoids reading 930 files)
    meta_path = os.path.join(day_dir, "_metadata.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path) as mf:
                meta = json.load(mf)
            inst_map = meta.get("instruments", {})
            if inst_map:
                instruments = {}
                for inst_key, description in inst_map.items():
                    parts = description.split()
                    if len(parts) < 2: continue
                    underlying = parts[0]
                    strike_kind = parts[1]
                    expiry = parts[2] if len(parts) >= 3 else ""
                    
                    if strike_kind == "FUT":
                        kind = "FUT"
                        strike = ""
                    elif strike_kind.endswith("CE"):
                        kind = "CE"
                        strike = strike_kind[:-2]
                    elif strike_kind.endswith("PE"):
                        kind = "PE"
                        strike = strike_kind[:-2]
                    else:
                        continue
                    
                    fname = None
                    for ext in ['.parquet', '.jsonl']:
                        for base in [day_dir, os.path.join(day_dir, underlying)]:
                            candidate = os.path.join(base, inst_key + ext)
                            if os.path.exists(candidate):
                                fname = candidate
                                break
                        if fname: break
                    
                    if not fname: continue
                    
                    try:
                        if fname.endswith('.parquet'):
                            import pyarrow.parquet as pq
                            tick_count = pq.read_metadata(fname).num_rows
                        else:
                            tick_count = max(1, os.path.getsize(fname) // 800)
                    except:
                        tick_count = 0
                    
                    instruments[inst_key] = {
                        "path": fname, "fname": os.path.basename(fname),
                        "key": inst_key, "name": description, "kind": kind,
                        "underlying": underlying, "strike": strike,
                        "expiry": expiry, "tick_count": tick_count,
                    }
                
                if instruments:
                    print(f"  Loaded {len(instruments)} instruments from _metadata.json (fast)")
                    return instruments
        except Exception as e:
            print(f"  ⚠ _metadata.json failed ({e}), scanning files...")

    instruments = {}

    # Collect JSONL files from root AND subdirectories
    jsonl_files = sorted(glob.glob(os.path.join(day_dir, "*.jsonl")))
    jsonl_files += sorted(glob.glob(os.path.join(day_dir, "*", "*.jsonl")))
    jsonl_files += sorted(glob.glob(os.path.join(day_dir, "*.parquet")))
    jsonl_files += sorted(glob.glob(os.path.join(day_dir, "*", "*.parquet")))

    # Dedup: when both .jsonl and .parquet exist, prefer .parquet (smaller/faster)
    seen_bases = {}
    for fpath in jsonl_files:
        base = fpath.replace(".jsonl", "").replace(".parquet", "")
        if base not in seen_bases or fpath.endswith(".parquet"):
            seen_bases[base] = fpath
    jsonl_files = sorted(seen_bases.values())

    for fpath in jsonl_files:
        fname = os.path.basename(fpath)

        # Skip metadata/market_info files
        if fname.startswith("_"):
            continue
        # Read first record to get the instrument key
        try:
            if fpath.endswith(".parquet"):
                import pyarrow.parquet as pq
                pf = pq.ParquetFile(fpath)
                first_batch = pf.read_row_group(0, columns=["raw"]).column("raw")
                first_raw = first_batch[0].as_py()
                rec = json.loads(first_raw)
                key = rec.get("key", "")
            else:
                with open(fpath) as f:
                    first_line = f.readline().strip()
                    if not first_line:
                        continue
                    rec = json.loads(first_line)
                    key = rec.get("key", "")
        except Exception:
            continue

        # Build a readable name from filename
        # Harvest format: "BANKNIFTY_57500CE_2026-08-25__NSE_FO_59090.jsonl"
        # Live format:    "NIFTY_24250CE_NSE_FO_61671.jsonl"
        name = fname.replace(".jsonl", "").replace(".parquet", "")

        # Detect parent folder for underlying (BANKNIFTY, NIFTY, SENSEX)
        parent_dir = os.path.basename(os.path.dirname(fpath))
        parent_upper = parent_dir.upper()

        # Classify instrument type
        # CE/PE files always have a strike number before CE/PE: "57500CE_", "24250PE_"
        # Use regex to avoid false positives from words like "SPACE_", "PRICE_"
        name_upper = fname.upper()
        kind = "OTHER"
        if "_FUT_" in name_upper or name_upper.startswith("FUT"):
            kind = "FUT"
        elif re.search(r"\dCE[_.]", name_upper):
            kind = "CE"
        elif re.search(r"\dPE[_.]", name_upper):
            kind = "PE"
        elif "INDEX" in name_upper or "INDEX" in key.upper():
            kind = "INDEX"

        # Extract underlying from parent folder or filename
        # Parent folder is most reliable for harvest data (BANKNIFTY, NIFTY, SENSEX)
        if parent_upper in ("BANKNIFTY", "NIFTY", "SENSEX", "FINNIFTY", "MIDCPNIFTY", "BANKEX"):
            underlying = parent_upper
        else:
            # Fallback: extract from filename
            underlying = fname.split("_")[0].upper()

        # Extract strike if CE/PE
        # Harvest: "BANKNIFTY_57500CE_2026-08-25__NSE_FO_59090.jsonl" → 57500
        # Live:    "NIFTY_24250CE_NSE_FO_61671.jsonl" → 24250
        strike = None
        m = re.search(r"(\d{4,6})(CE|PE)", fname.upper())
        if m:
            strike = float(m.group(1))

        # Extract expiry if present in filename
        expiry = None
        m_exp = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
        if m_exp:
            expiry = m_exp.group(1)

        # Count ticks — fast method
        try:
            if fpath.endswith(".parquet"):
                import pyarrow.parquet as pq
                meta = pq.read_metadata(fpath)
                tick_count = meta.num_rows
            else:
                tick_count = max(1, os.path.getsize(fpath) // 800)
        except Exception:
            tick_count = 0

        instruments[fpath] = {
            "path": fpath,
            "fname": fname,
            "key": key,
            "name": name,
            "kind": kind,
            "underlying": underlying,
            "strike": strike,
            "expiry": expiry,
            "ticks": tick_count,
        }

    return instruments


_tick_cache = {}  # filepath → ticks (shared across combos)

def load_ticks(filepath, start_time=None, end_time=None):
    """Load ticks from a JSONL or Parquet file. Caches results for reuse.
    Returns list of (ts, key, data)."""
    global _tick_cache
    
    # Check for Parquet version (compressed)
    parquet_path = filepath.replace(".jsonl", ".parquet")
    if os.path.exists(parquet_path) and not os.path.exists(filepath):
        filepath = parquet_path
    
    cache_key = filepath
    if cache_key in _tick_cache:
        ticks = _tick_cache[cache_key]
        if start_time or end_time:
            return [(ts,k,d) for ts,k,d in ticks if
                    (not start_time or (len(ts)>15 and ts[11:16]>=start_time)) and
                    (not end_time or (len(ts)>15 and ts[11:16]<=end_time))]
        return ticks
    
    # Use fastest available JSON parser
    try:
        import orjson
        _loads = orjson.loads
    except ImportError:
        try:
            import ujson
            _loads = ujson.loads
        except ImportError:
            _loads = json.loads

    ticks = []
    
    if filepath.endswith(".parquet"):
        try:
            import pyarrow.parquet as pq
        except ImportError:
            print(f"  ⚠ Cannot read {os.path.basename(filepath)}: pip install pyarrow")
            return []
        try:
            table = pq.read_table(filepath)
            raw_col = table.column("raw").to_pylist()
            for raw in raw_col:
                try:
                    rec = _loads(raw)
                    ts = rec.get("ts", "")
                    key = rec.get("key", "")
                    data = rec.get("data", {})
                    ticks.append((ts, key, data))
                except Exception:
                    continue
            _tick_cache[cache_key] = ticks
            if start_time or end_time:
                return [(ts,k,d) for ts,k,d in ticks if
                        (not start_time or (len(ts)>15 and ts[11:16]>=start_time)) and
                        (not end_time or (len(ts)>15 and ts[11:16]<=end_time))]
            return ticks
        except Exception as e:
            print(f"  ⚠ Error reading {os.path.basename(filepath)}: {e}")
            return []

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = _loads(line)
                ts = rec.get("ts", "")
                key = rec.get("key", "")
                data = rec.get("data", {})
                ticks.append((ts, key, data))
            except Exception:
                continue
    
    _tick_cache[cache_key] = ticks
    
    if start_time or end_time:
        return [(ts,k,d) for ts,k,d in ticks if
                (not start_time or (len(ts)>15 and ts[11:16]>=start_time)) and
                (not end_time or (len(ts)>15 and ts[11:16]<=end_time))]
    return ticks


def clear_tick_cache():
    """Clear cached ticks to free memory between days."""
    global _tick_cache
    _tick_cache.clear()


def merge_ticks(*tick_lists):
    """Merge multiple pre-sorted tick lists by timestamp. Uses heapq for efficiency."""
    import heapq
    # heapq.merge requires sorted inputs — tick files are written in order,
    # but add fallback sort if any input is unsorted
    sorted_lists = []
    for tl in tick_lists:
        if tl and len(tl) > 1:
            # Quick check: is first < last?
            if tl[0][0] > tl[-1][0]:
                tl = sorted(tl, key=lambda x: x[0])
        sorted_lists.append(tl)
    return list(heapq.merge(*sorted_lists, key=lambda x: x[0]))


# ═══════════════════════════════════════════════════════════════
# STRATEGY 2: TREND FOLLOWING (one trade per day, big moves)
# ═══════════════════════════════════════════════════════════════

class TrendFollowing:
    """
    Trend Following strategy — one trade per day max.
    
    Logic:
    - Record the future price at a reference time (default 10:00 AM)
    - If by entry_time (default 10:30), future moved breakout_pts+ in one direction → buy option
    - CALL if future went UP, PUT if future went DOWN
    - Fixed stop loss, fixed target, time-based exit at exit_time
    - Maximum 1 trade per day
    """
    def __init__(self, side=None, ref_time="10:00", entry_time="10:30",
                 exit_time="13:00", breakout_pts=100,
                 stop_pct=30, target_pct=80):
        self.side = side  # None = auto-detect from future direction
        self.ref_time = ref_time        # when to record the future's reference price
        self.entry_time = entry_time    # when to check for breakout and enter
        self.exit_time = exit_time      # time-based exit (latest)
        self.breakout_pts = breakout_pts  # future must move this many pts from ref price
        self.stop_pct = stop_pct        # stop loss as % of entry price
        self.target_pct = target_pct    # profit target as % of entry price

        # State
        self.fut_ref_price = None       # future price at ref_time
        self.fut_ltp = None
        self.opt_ltp = None
        self.entry_price = None
        self.stop_loss = None
        self.target_price = None
        self._in_position = False
        self._traded_today = False      # only 1 trade per day
        self._ref_captured = False
        self._entry_window_open = False
        self._direction = None          # "CALL" or "PUT"
        self._current_date = None

    def on_future_tick(self, ltp, timestamp, feed_data=None):
        ltp = float(ltp)
        self.fut_ltp = ltp
        
        time_str = timestamp[11:16] if len(timestamp) > 15 else ""
        date_str = timestamp[:10] if len(timestamp) > 9 else ""
        
        # Reset on new day
        if date_str != self._current_date:
            self._current_date = date_str
            self.fut_ref_price = None
            self._ref_captured = False
            self._entry_window_open = False
            self._traded_today = False
            self._direction = None
        
        # Capture reference price at ref_time
        if not self._ref_captured and time_str >= self.ref_time:
            self.fut_ref_price = ltp
            self._ref_captured = True
        
        # Open entry window at entry_time
        if self._ref_captured and not self._entry_window_open and time_str >= self.entry_time:
            self._entry_window_open = True
        
        return None

    def on_option_tick(self, ltp, timestamp, feed_data=None):
        ltp = float(ltp)
        self.opt_ltp = ltp
        
        time_str = timestamp[11:16] if len(timestamp) > 15 else ""
        
        # ── In position: check stop, target, time exit ──
        if self._in_position:
            # Stop loss hit
            if ltp <= self.stop_loss:
                self._in_position = False
                return "SELL"
            # Target hit
            if ltp >= self.target_price:
                self._in_position = False
                return "SELL"
            # Time exit
            if time_str >= self.exit_time:
                self._in_position = False
                return "SELL"
            return None
        
        # ── Not in position: check for entry ──
        if self._traded_today:
            return None  # already traded today
        if not self._entry_window_open:
            return None  # waiting for entry window
        if self.fut_ref_price is None or self.fut_ltp is None:
            return None
        
        # Check breakout direction
        fut_move = self.fut_ltp - self.fut_ref_price
        
        if self.side:
            # Fixed side mode
            direction = self.side
            if direction == "CALL" and fut_move < self.breakout_pts:
                return None
            if direction == "PUT" and fut_move > -self.breakout_pts:
                return None
        else:
            # Auto-detect direction
            if fut_move >= self.breakout_pts:
                direction = "CALL"
            elif fut_move <= -self.breakout_pts:
                direction = "PUT"
            else:
                return None  # no breakout yet
        
        self._direction = direction
        
        # Don't enter after exit_time
        if time_str >= self.exit_time:
            return None
        
        # ENTRY
        self._in_position = True
        self._traded_today = True
        self.entry_price = ltp
        self.stop_loss = ltp * (1 - self.stop_pct / 100)
        self.target_price = ltp * (1 + self.target_pct / 100)
        return "BUY"


def run_trend_backtest(ce_file, pe_file, fut_file, ce_info, pe_info, fut_info,
                       ref_time="10:00", entry_time="10:30", exit_time="13:00",
                       breakout_pts=100, stop_pct=30, target_pct=80,
                       lot_size=1, quiet=False):
    """
    Run trend-following strategy backtest.
    Only ONE trade per day — either CE or PE based on future direction.
    """
    ce_name = ce_info["name"]
    pe_name = pe_info["name"]
    fut_name = fut_info["name"]
    ce_key = ce_info["key"]
    pe_key = pe_info["key"]
    fut_key = fut_info["key"]

    if not quiet:
        print(f"  ── Trend Backtest: {ce_name} + {pe_name} ──")
        print(f"  Future: {fut_name}")
        print(f"  Ref: {ref_time} │ Entry: {entry_time} │ Exit: {exit_time}")
        print(f"  Breakout: {breakout_pts} pts │ Stop: {stop_pct}% │ Target: {target_pct}%")
        print(f"  Loading ticks...")

    ce_ticks = load_ticks(ce_file)
    pe_ticks = load_ticks(pe_file)
    fut_ticks = load_ticks(fut_file)

    if not quiet:
        print(f"  CE: {len(ce_ticks):,} ticks │ PE: {len(pe_ticks):,} ticks │ FUT: {len(fut_ticks):,} ticks")

    all_ticks = merge_ticks(ce_ticks, pe_ticks, fut_ticks)
    if not all_ticks:
        return None

    # Create strategy instances for both sides
    ce_strat = TrendFollowing(side="CALL", ref_time=ref_time, entry_time=entry_time,
                               exit_time=exit_time, breakout_pts=breakout_pts,
                               stop_pct=stop_pct, target_pct=target_pct)
    pe_strat = TrendFollowing(side="PUT", ref_time=ref_time, entry_time=entry_time,
                               exit_time=exit_time, breakout_pts=breakout_pts,
                               stop_pct=stop_pct, target_pct=target_pct)

    ce_trader = PaperTrader(ce_name, lot_size=lot_size, max_trades=5)
    pe_trader = PaperTrader(pe_name, lot_size=lot_size, max_trades=5)

    # Replay ticks
    for ts, key, data in all_ticks:
        ltp = extract_ltp(data)
        if ltp is None:
            continue

        if key == fut_key:
            ce_strat.on_future_tick(ltp, ts, data)
            pe_strat.on_future_tick(ltp, ts, data)
        elif key == ce_key:
            signal = ce_strat.on_option_tick(ltp, ts, data)
            # Check if future confirms CALL direction
            if signal == "BUY" and ce_strat._direction == "CALL":
                if not ce_trader.in_position:
                    ce_trader.buy(ltp, ts)
            elif signal == "SELL" and ce_trader.in_position:
                ce_trader.sell(ltp, ts)
                ce_trader.last_exit_bev = False
                ce_trader.last_exit_wide = False
                ce_trader.last_exit_peak = ce_strat.entry_price
        elif key == pe_key:
            signal = pe_strat.on_option_tick(ltp, ts, data)
            # Check if future confirms PUT direction
            if signal == "BUY" and pe_strat._direction == "PUT":
                if not pe_trader.in_position:
                    pe_trader.buy(ltp, ts)
            elif signal == "SELL" and pe_trader.in_position:
                pe_trader.sell(ltp, ts)
                pe_trader.last_exit_bev = False
                pe_trader.last_exit_wide = False
                pe_trader.last_exit_peak = pe_strat.entry_price

    # Force close any open positions
    if ce_trader.in_position and ce_trader.current_ltp:
        ce_trader.sell(ce_trader.current_ltp, ts)
    if pe_trader.in_position and pe_trader.current_ltp:
        pe_trader.sell(pe_trader.current_ltp, ts)

    ce_pnl = sum(t["pnl"] for t in ce_trader.trades)
    pe_pnl = sum(t["pnl"] for t in pe_trader.trades)
    total_pnl = ce_pnl + pe_pnl
    total_trades = len(ce_trader.trades) + len(pe_trader.trades)
    total_wins = sum(1 for t in ce_trader.trades + pe_trader.trades if t["pnl"] > 0)

    if not quiet:
        print(f"\n  ══════════════════════════════════════════════════════════════")
        print(f"  TREND FOLLOWING RESULTS")
        print(f"  ══════════════════════════════════════════════════════════════")
        print(f"  CE: {ce_name}")
        print(f"  PE: {pe_name}")
        print(f"  FUT: {fut_name}")
        print(f"  Ref: {ref_time} │ Entry after: {entry_time} │ Exit by: {exit_time}")
        print(f"  Breakout: {breakout_pts} pts │ Stop: {stop_pct}% │ Target: {target_pct}%")
        print(f"  Fut ref price: {'₹'+f'{ce_strat.fut_ref_price:.2f}' if ce_strat.fut_ref_price else 'N/A'}")
        print(f"\n  ┌────────────┬──────────┬──────────┬──────────┐")
        print(f"  │            │  Trades  │   Wins   │  P&L ₹   │")
        print(f"  ├────────────┼──────────┼──────────┼──────────┤")
        ce_w = sum(1 for t in ce_trader.trades if t["pnl"] > 0)
        pe_w = sum(1 for t in pe_trader.trades if t["pnl"] > 0)
        print(f"  │ CE (CALL)  │ {len(ce_trader.trades):>5}   │ {ce_w:>5}   │ {'+' if ce_pnl>=0 else ''}{ce_pnl:>8.2f}  │")
        print(f"  │ PE (PUT)   │ {len(pe_trader.trades):>5}   │ {pe_w:>5}   │ {'+' if pe_pnl>=0 else ''}{pe_pnl:>8.2f}  │")
        print(f"  ├────────────┼──────────┼──────────┼──────────┤")
        print(f"  │ COMBINED   │ {total_trades:>5}   │ {total_wins:>5}   │ {'+' if total_pnl>=0 else ''}{total_pnl:>8.2f}  │")
        wr = total_wins/total_trades*100 if total_trades > 0 else 0
        print(f"  │ Win rate   │          │ {wr:>5.1f}%  │          │")
        print(f"  └────────────┴──────────┴──────────┴──────────┘")

        # Show trades
        all_trades = []
        for t in ce_trader.trades:
            all_trades.append(("CE", t))
        for t in pe_trader.trades:
            all_trades.append(("PE", t))
        all_trades.sort(key=lambda x: x[1]["entry_time"])

        if all_trades:
            print(f"\n  ── Trade Details ──")
            print(f"  {'#':<4} {'Side':<5} {'BuyTime':>8} {'SellTm':>8} {'Entry':>8} {'Peak':>8} {'Exit':>8} {'P&L':>8} {'%':>7} {'Reason':<8}")
            print(f"  {'─'*75}")
            for i, (side, t) in enumerate(all_trades, 1):
                sign = "+" if t["pnl"] >= 0 else ""
                et = t["entry_time"][11:19] if len(t["entry_time"]) > 18 else t["entry_time"][-8:]
                xt = t["exit_time"][11:19] if len(t["exit_time"]) > 18 else t["exit_time"][-8:]
                peak = t.get("peak_price", t["entry_price"])
                # Determine exit reason
                pct_gain = (t["exit_price"] - t["entry_price"]) / t["entry_price"] * 100
                if pct_gain <= -stop_pct * 0.95:
                    reason = "STOP"
                elif pct_gain >= target_pct * 0.95:
                    reason = "TARGET"
                else:
                    reason = "TIME"
                print(f"  {i:<4} {side:<5} {et:>8} {xt:>8} {t['entry_price']:>8.2f} {peak:>8.2f} {t['exit_price']:>8.2f} "
                      f"{sign}{t['pnl']:>7.2f} {sign}{t['pnl']/t['entry_price']*100:>6.1f}% {reason:<8}")

    return {
        "ce_trades": ce_trader.trades, "pe_trades": pe_trader.trades,
        "ce_pnl": ce_pnl, "pe_pnl": pe_pnl, "total_pnl": total_pnl,
        "total_trades": total_trades, "total_wins": total_wins,
        "fut_ref_price": ce_strat.fut_ref_price,
    }


# ═══════════════════════════════════════════════════════════════
# BACKTEST RUNNER (Original Momentum Strategy)
# ═══════════════════════════════════════════════════════════════

def run_backtest(ce_file, pe_file, fut_file, ce_info, pe_info, fut_info,
                 opt_low_margin=5, fut_low_margin=10, trail_tiers=None,
                 bev_trigger=2.0, bev_buffer=1.0, bev_buffer_mode="rupees", straight_down_pct=25.0,
                 max_trades=150, lot_size=1, quiet=False,
                 start_time="09:15", end_time="15:30", fut_move_limit=-1,
                 opt_open_rally_pct=50, opt_atp_distance_pct=30, opt_buy_sell_ratio=0.0,
                 rally_base="daylow_prevclose", verbose=False, minute_interval=1,
                 opt_momentum_threshold=1.0, first_only=False, target_profit=0,
                 require_lower_wick=False, block_marubozu=0, max_hold=0,
                 max_range=0, max_momentum=0, max_above_high=0, rebuy_drop=0,
                 min_premium=0, max_premium=0, side_filter="BOTH"):
    """
    Run dual CE+PE strategy backtest on saved data.
    Returns dict with results for both sides.
    """
    ce_name = ce_info["name"]
    pe_name = pe_info["name"]
    fut_name = fut_info["name"]
    ce_key = ce_info["key"]
    pe_key = pe_info["key"]
    fut_key = fut_info["key"]

    if not quiet:
        print(f"\n  ── Backtest: {ce_name} + {pe_name} ──")
        print(f"  Future: {fut_name}")
        print(f"  Trail: {trail_tiers or [(0,999),(2,1),(6,2),(10,3),(15,5)]} │ BEV: {bev_trigger}%/{bev_buffer}{'%' if bev_buffer_mode=='pct' else '₹'} │ Wide: {straight_down_pct}% │ FutLimit: {fut_move_limit}")
        print(f"  OpenRally: {opt_open_rally_pct}% │ ATP dist: {opt_atp_distance_pct}% │ B/S ratio: {opt_buy_sell_ratio}")
        print(f"  Time filter: {start_time} → {end_time}")
        print(f"  Loading ticks...")

    # Load and merge all ticks in timestamp order (filtered during load for speed)
    ce_ticks = load_ticks(ce_file, start_time=start_time, end_time=end_time)
    pe_ticks = load_ticks(pe_file, start_time=start_time, end_time=end_time)
    fut_ticks = load_ticks(fut_file, start_time=start_time, end_time=end_time)

    if not quiet:
        print(f"  CE: {len(ce_ticks):,} ticks │ PE: {len(pe_ticks):,} ticks │ FUT: {len(fut_ticks):,} ticks")

    all_ticks = merge_ticks(ce_ticks, pe_ticks, fut_ticks)
    if not all_ticks:
        print(f"  ⚠ No ticks loaded.")
        return None

    if not quiet:
        print(f"  Total: {len(all_ticks):,} ticks │ {all_ticks[0][0][:19]} → {all_ticks[-1][0][:19]}")
        print(f"  Running strategy...")

    # Create strategy instances
    call_strat = OptionFutureMomentum(side="CALL", opt_low_margin=opt_low_margin,
                                       fut_low_margin=fut_low_margin, trail_tiers=trail_tiers,
                                       bev_trigger=bev_trigger, bev_buffer=bev_buffer,
                                       bev_buffer_mode=bev_buffer_mode,
                                       straight_down_pct=straight_down_pct, fut_move_limit=fut_move_limit,
                                       opt_open_rally_pct=opt_open_rally_pct, opt_atp_distance_pct=opt_atp_distance_pct,
                                       opt_buy_sell_ratio=opt_buy_sell_ratio, rally_base=rally_base,
                                       minute_interval=minute_interval,
                                       opt_momentum_threshold=opt_momentum_threshold)
    put_strat = OptionFutureMomentum(side="PUT", opt_low_margin=opt_low_margin,
                                      fut_low_margin=fut_low_margin, trail_tiers=trail_tiers,
                                      bev_trigger=bev_trigger, bev_buffer=bev_buffer,
                                      bev_buffer_mode=bev_buffer_mode,
                                      straight_down_pct=straight_down_pct, fut_move_limit=fut_move_limit,
                                      opt_open_rally_pct=opt_open_rally_pct, opt_atp_distance_pct=opt_atp_distance_pct,
                                      opt_buy_sell_ratio=opt_buy_sell_ratio, rally_base=rally_base,
                                      minute_interval=minute_interval,
                                      opt_momentum_threshold=opt_momentum_threshold)

    call_strat._verbose = verbose
    put_strat._verbose = verbose
    call_trader = PaperTrader(ce_name, lot_size=lot_size, max_trades=max_trades)
    put_trader = PaperTrader(pe_name, lot_size=lot_size, max_trades=max_trades)

    # Track if first trade taken per side (for --first-only mode)
    ce_first_traded = False
    pe_first_traded = False
    # Track last entry price for --rebuy-drop (require dip before re-entry)
    ce_last_entry = None  # last CE buy price
    pe_last_entry = None

    # Replay ticks
    errors = 0
    for ts, key, data in all_ticks:
        try:
            ltp = extract_ltp(data)
            if ltp is None or ltp <= 0:
                continue

            if key == ce_key:
                call_trader.update_tick(ltp, ts)
                if call_trader.in_position and ltp > (call_trader.peak_price or 0):
                    call_trader.peak_price = ltp
                # Rebuy-drop: track if LTP has dropped enough since last entry
                # Target profit exit: sell when LTP >= entry + target%
                if target_profit > 0 and call_trader.in_position and call_trader.entry_price:
                    target_price = call_trader.entry_price * (1 + target_profit / 100)
                    if ltp >= target_price:
                        entry_was = call_trader.entry_price
                        call_trader.sell(ltp, ts, peak=call_trader.peak_price,
                                         bev=False, wide=False, trail=True)
                        # Reset strategy state so it can buy again
                        call_strat._in_position = False; call_strat.entry_price = None
                        call_strat.peak_price = None; call_strat.stop_loss = None
                        call_strat._breakeven_active = False; call_strat._breakeven_stop = None
                        call_strat._sell_minute = ts[11:16] if len(ts) > 15 else None
                        if not quiet:
                            print(f"  ✓ CE TARGET HIT +{target_profit}%: entry={entry_was:.0f} exit={ltp:.0f} P&L=+{ltp-entry_was:.0f}")
                        continue
                # Max hold time exit: only if BEV never activated (trade never reached +2%)
                if max_hold > 0 and call_trader.in_position and not call_strat._breakeven_active and target_profit <= 0 and call_trader.entry_time:
                    try:
                        et = call_trader.entry_time
                        eh,em = int(et[11:13]),int(et[14:16])
                        th,tm = int(ts[11:13]),int(ts[14:16])
                        held = (th*60+tm)-(eh*60+em)
                        if held >= max_hold:
                            call_trader.sell(ltp, ts, peak=call_trader.peak_price,
                                             bev=False, wide=False, trail=False)
                            call_strat._in_position = False; call_strat.entry_price = None
                            call_strat.peak_price = None; call_strat.stop_loss = None
                            call_strat._breakeven_active = False; call_strat._breakeven_stop = None
                            call_strat._sell_minute = ts[11:16] if len(ts) > 15 else None
                            continue
                    except: pass
                signal = call_strat.on_option_tick(ltp, ts, data)
                if signal == "BUY" and call_trader.can_buy():
                    # Side filter
                    if side_filter == "PE":
                        call_strat._in_position = False; call_strat.entry_price = None; call_strat.peak_price = None; call_strat.stop_loss = None
                        continue
                    # Premium check removed — handled by pre-filter at day start
                    if first_only and ce_first_traded:
                        call_strat._in_position = False; call_strat.entry_price = None; call_strat.peak_price = None; call_strat.stop_loss = None
                        continue
                    # Rebuy-drop: buy only if LTP is at or below threshold
                    if rebuy_drop > 0 and ce_last_entry is not None:
                        if ltp > ce_last_entry * (1 - rebuy_drop / 100):
                            call_strat._in_position = False; call_strat.entry_price = None; call_strat.peak_price = None; call_strat.stop_loss = None
                            continue
                    # OHLC entry filters
                    ohlc_block = False
                    O = call_strat.opt_prev_min_open
                    H = call_strat.opt_prev_min_high
                    L = call_strat.opt_prev_min_low
                    C = call_strat.opt_prev_min_close
                    A = call_strat.opt_prev_avg_ltp
                    if O is not None and H is not None and L is not None and C is not None:
                        rng = H - L
                        lower_wick = min(O, C) - L
                        body_ratio = abs(C - O) / rng if rng > 0 else 0
                        if require_lower_wick and lower_wick < 0.01:
                            ohlc_block = True
                        if block_marubozu > 0 and body_ratio >= block_marubozu:
                            ohlc_block = True
                        if max_range > 0 and rng > max_range:
                            ohlc_block = True
                    if max_momentum > 0 and A is not None and ltp > A + max_momentum:
                        ohlc_block = True
                    if max_above_high > 0 and H is not None and ltp > H + max_above_high:
                        ohlc_block = True
                    if ohlc_block:
                        call_strat._in_position = False; call_strat.entry_price = None; call_strat.peak_price = None; call_strat.stop_loss = None
                        continue
                    prevmin = None
                    if call_strat.opt_prev_min_open is not None:
                        prevmin = {"O": call_strat.opt_prev_min_open, "H": call_strat.opt_prev_min_high,
                                   "L": call_strat.opt_prev_min_low, "C": call_strat.opt_prev_min_close,
                                   "A": call_strat.opt_prev_avg_ltp}
                    ce_greeks = {"delta": extract_greek(data, "delta"), "theta": extract_greek(data, "theta"),
                                 "gamma": extract_greek(data, "gamma"), "vega": extract_greek(data, "vega"),
                                 "iv": extract_greek(data, "iv")}
                    call_trader.buy(ltp, ts, prevmin_ohlc=prevmin, entry_greeks=ce_greeks, entry_fut=call_strat.fut_ltp)
                    ce_first_traded = True
                    ce_last_entry = ltp
                elif signal == "SELL" and call_trader.can_sell():
                    if target_profit > 0:
                        # Strategy triggered a stop but we're ignoring it (waiting for target).
                        # Re-sync strategy: put it back in position so it keeps tracking.
                        call_strat._in_position = True
                        call_strat.entry_price = call_trader.entry_price
                        call_strat.peak_price = call_trader.peak_price
                        call_strat.stop_loss = call_strat.entry_price * (1 - call_strat.straight_down_stop_pct / 100) if call_strat.entry_price else None
                        continue  # ignore normal sell — wait for target
                    call_trader.sell(ltp, ts,
                        peak=call_strat.last_exit_peak,
                        bev=call_strat.last_exit_bev,
                        wide=call_strat.last_exit_wide,
                        trail=call_strat.last_exit_trail)

            elif key == pe_key:
                put_trader.update_tick(ltp, ts)
                if put_trader.in_position and ltp > (put_trader.peak_price or 0):
                    put_trader.peak_price = ltp
                # Rebuy-drop: track if LTP has dropped enough since last entry
                # Target profit exit
                if target_profit > 0 and put_trader.in_position and put_trader.entry_price:
                    target_price = put_trader.entry_price * (1 + target_profit / 100)
                    if ltp >= target_price:
                        entry_was = put_trader.entry_price
                        put_trader.sell(ltp, ts, peak=put_trader.peak_price,
                                        bev=False, wide=False, trail=True)
                        put_strat._in_position = False; put_strat.entry_price = None
                        put_strat.peak_price = None; put_strat.stop_loss = None
                        put_strat._breakeven_active = False; put_strat._breakeven_stop = None
                        put_strat._sell_minute = ts[11:16] if len(ts) > 15 else None
                        if not quiet:
                            print(f"  ✓ PE TARGET HIT +{target_profit}%: entry={entry_was:.0f} exit={ltp:.0f} P&L=+{ltp-entry_was:.0f}")
                        continue
                # Max hold time exit for PE
                if max_hold > 0 and put_trader.in_position and not put_strat._breakeven_active and target_profit <= 0 and put_trader.entry_time:
                    try:
                        et = put_trader.entry_time
                        eh,em = int(et[11:13]),int(et[14:16])
                        th,tm = int(ts[11:13]),int(ts[14:16])
                        held = (th*60+tm)-(eh*60+em)
                        if held >= max_hold:
                            put_trader.sell(ltp, ts, peak=put_trader.peak_price,
                                            bev=False, wide=False, trail=False)
                            put_strat._in_position = False; put_strat.entry_price = None
                            put_strat.peak_price = None; put_strat.stop_loss = None
                            put_strat._breakeven_active = False; put_strat._breakeven_stop = None
                            put_strat._sell_minute = ts[11:16] if len(ts) > 15 else None
                            continue
                    except: pass
                signal = put_strat.on_option_tick(ltp, ts, data)
                if signal == "BUY" and put_trader.can_buy():
                    # Side filter
                    if side_filter == "CE":
                        put_strat._in_position = False; put_strat.entry_price = None; put_strat.peak_price = None; put_strat.stop_loss = None
                        continue
                    # Premium check removed — handled by pre-filter at day start
                    if first_only and pe_first_traded:
                        put_strat._in_position = False; put_strat.entry_price = None; put_strat.peak_price = None; put_strat.stop_loss = None
                        continue
                    # Rebuy-drop: buy only if LTP is at or below threshold
                    if rebuy_drop > 0 and pe_last_entry is not None:
                        if ltp > pe_last_entry * (1 - rebuy_drop / 100):
                            put_strat._in_position = False; put_strat.entry_price = None; put_strat.peak_price = None; put_strat.stop_loss = None
                            continue
                    # OHLC entry filters
                    ohlc_block = False
                    O = put_strat.opt_prev_min_open
                    H = put_strat.opt_prev_min_high
                    L = put_strat.opt_prev_min_low
                    C = put_strat.opt_prev_min_close
                    A = put_strat.opt_prev_avg_ltp
                    if O is not None and H is not None and L is not None and C is not None:
                        rng = H - L
                        lower_wick = min(O, C) - L
                        body_ratio = abs(C - O) / rng if rng > 0 else 0
                        if require_lower_wick and lower_wick < 0.01:
                            ohlc_block = True
                        if block_marubozu > 0 and body_ratio >= block_marubozu:
                            ohlc_block = True
                        if max_range > 0 and rng > max_range:
                            ohlc_block = True
                    if max_momentum > 0 and A is not None and ltp > A + max_momentum:
                        ohlc_block = True
                    if max_above_high > 0 and H is not None and ltp > H + max_above_high:
                        ohlc_block = True
                    if ohlc_block:
                        put_strat._in_position = False; put_strat.entry_price = None; put_strat.peak_price = None; put_strat.stop_loss = None
                        continue
                    prevmin = None
                    if put_strat.opt_prev_min_open is not None:
                        prevmin = {"O": put_strat.opt_prev_min_open, "H": put_strat.opt_prev_min_high,
                                   "L": put_strat.opt_prev_min_low, "C": put_strat.opt_prev_min_close,
                                   "A": put_strat.opt_prev_avg_ltp}
                    pe_greeks = {"delta": extract_greek(data, "delta"), "theta": extract_greek(data, "theta"),
                                 "gamma": extract_greek(data, "gamma"), "vega": extract_greek(data, "vega"),
                                 "iv": extract_greek(data, "iv")}
                    put_trader.buy(ltp, ts, prevmin_ohlc=prevmin, entry_greeks=pe_greeks, entry_fut=put_strat.fut_ltp)
                    pe_first_traded = True
                    pe_last_entry = ltp
                elif signal == "SELL" and put_trader.can_sell():
                    if target_profit > 0:
                        put_strat._in_position = True
                        put_strat.entry_price = put_trader.entry_price
                        put_strat.peak_price = put_trader.peak_price
                        put_strat.stop_loss = put_strat.entry_price * (1 - put_strat.straight_down_stop_pct / 100) if put_strat.entry_price else None
                        continue  # ignore normal sell — wait for target
                    put_trader.sell(ltp, ts,
                        peak=put_strat.last_exit_peak,
                        bev=put_strat.last_exit_bev,
                        wide=put_strat.last_exit_wide,
                        trail=put_strat.last_exit_trail)

            elif key == fut_key:
                call_strat.on_future_tick(ltp, ts, data)
                put_strat.on_future_tick(ltp, ts, data)

        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  ⚠ Tick error: {e}")

    # Close any open positions at last known LTP (TIME exit — end of day)
    if call_trader.in_position and call_trader.current_ltp:
        call_trader.sell(call_trader.current_ltp, all_ticks[-1][0],
                         peak=call_trader.peak_price, bev=False, wide=False, trail=False)
    if put_trader.in_position and put_trader.current_ltp:
        put_trader.sell(put_trader.current_ltp, all_ticks[-1][0],
                         peak=put_trader.peak_price, bev=False, wide=False, trail=False)

    # Compute results
    ce_pnl = sum(t["pnl"] for t in call_trader.trades)
    pe_pnl = sum(t["pnl"] for t in put_trader.trades)
    ce_wins = sum(1 for t in call_trader.trades if t["pnl"] > 0)
    pe_wins = sum(1 for t in put_trader.trades if t["pnl"] > 0)

    result = {
        "ce_name": ce_name,
        "pe_name": pe_name,
        "fut_name": fut_name,
        "ce_trades": call_trader.trades,
        "pe_trades": put_trader.trades,
        "ce_pnl": round(ce_pnl, 2),
        "pe_pnl": round(pe_pnl, 2),
        "total_pnl": round(ce_pnl + pe_pnl, 2),
        "ce_wins": ce_wins,
        "pe_wins": pe_wins,
        "ce_count": len(call_trader.trades),
        "pe_count": len(put_trader.trades),
        "bev_count": sum(1 for t in call_trader.trades + put_trader.trades if t.get("bev")),
        "wide_count": sum(1 for t in call_trader.trades + put_trader.trades if t.get("wide")),
        "trail_count": sum(1 for t in call_trader.trades + put_trader.trades if t.get("trail")),
        "total_ticks": len(all_ticks),
        "errors": errors,
        "time_range": f"{all_ticks[0][0][:19]} → {all_ticks[-1][0][:19]}",
        "config": f"Trail: {trail_tiers} │ BEV: {bev_trigger}%/{bev_buffer}₹ │ Wide: {straight_down_pct}% │ FutLimit: {fut_move_limit}",
    }

    return result


def print_result(result, show_trades=True):
    """Pretty-print backtest results."""
    if not result:
        return

    total = result["ce_count"] + result["pe_count"]
    total_wins = result["ce_wins"] + result["pe_wins"]
    win_rate = (total_wins / total * 100) if total > 0 else 0

    print(f"\n  {'═' * 62}")
    print(f"  BACKTEST RESULTS")
    print(f"  {'═' * 62}")
    print(f"  CE: {result['ce_name']}")
    print(f"  PE: {result['pe_name']}")
    print(f"  FUT: {result['fut_name']}")
    print(f"  Time: {result['time_range']}")
    print(f"  Ticks: {result['total_ticks']:,} │ Errors: {result['errors']}")
    if result.get("config"):
        print(f"  {result['config']}")
    print()

    # Summary boxes
    print(f"  ┌────────────┬──────────┬──────────┬──────────┐")
    print(f"  │            │  Trades  │   Wins   │  P&L ₹   │")
    print(f"  ├────────────┼──────────┼──────────┼──────────┤")
    ce_sign = "+" if result["ce_pnl"] >= 0 else ""
    pe_sign = "+" if result["pe_pnl"] >= 0 else ""
    t_sign = "+" if result["total_pnl"] >= 0 else ""
    print(f"  │ CE (CALL)  │  {result['ce_count']:>5}   │  {result['ce_wins']:>5}   │ {ce_sign}{result['ce_pnl']:>7.2f}  │")
    print(f"  │ PE (PUT)   │  {result['pe_count']:>5}   │  {result['pe_wins']:>5}   │ {pe_sign}{result['pe_pnl']:>7.2f}  │")
    print(f"  ├────────────┼──────────┼──────────┼──────────┤")
    print(f"  │ COMBINED   │  {total:>5}   │  {total_wins:>5}   │ {t_sign}{result['total_pnl']:>7.2f}  │")
    print(f"  │ Win rate   │          │ {win_rate:>5.1f}%  │          │")
    print(f"  │ BEV saves  │  {result.get('bev_count', 0):>5}   │          │          │")
    print(f"  │ TRAIL exits│  {result.get('trail_count', 0):>5}   │          │          │")
    print(f"  │ WIDE exits │  {result.get('wide_count', 0):>5}   │          │          │")
    print(f"  └────────────┴──────────┴──────────┴──────────┘")

    if show_trades and (result["ce_trades"] or result["pe_trades"]):
        print(f"\n  ── Trade Details ──")
        print(f"  {'#':<4} {'Side':<5} {'BuyTime':>8} {'SellTm':>8} {'Entry':>8} {'Peak':>8} {'Exit':>8} {'P&L':>8} {'%':>7} {'Tag':<5} {'':>2}PrevMin: O/H/L/C/Avg{'':>20}Fut{'':>5}Δ{'':>6}θ{'':>6}γ{'':>8}ν{'':>5}IV")
        print(f"  {'─'*165}")

        all_trades = []
        for t in result["ce_trades"]:
            all_trades.append(("CE", t))
        for t in result["pe_trades"]:
            all_trades.append(("PE", t))
        all_trades.sort(key=lambda x: x[1]["entry_time"])

        for i, (side, t) in enumerate(all_trades, 1):
            sign = "+" if t["pnl"] >= 0 else ""
            tag = "TRAIL" if t.get("trail") else ("BEV" if t.get("bev") else ("WIDE" if t.get("wide") else "TIME"))
            peak = t.get("peak_price", t["entry_price"])
            et = t["entry_time"][11:19] if len(t["entry_time"]) > 18 else t["entry_time"][-8:]
            xt = t["exit_time"][11:19] if len(t["exit_time"]) > 18 else t["exit_time"][-8:]
            pm = t.get("prevmin")
            pm_str = f"O={pm['O']:.0f} H={pm['H']:.0f} L={pm['L']:.0f} C={pm['C']:.0f} A={pm['A']:.0f}" if pm else "—"
            g = t.get("greeks") or {}
            ef = t.get("entry_fut")
            d = g.get("delta"); th = g.get("theta"); ga = g.get("gamma"); ve = g.get("vega"); iv = g.get("iv")
            gk_parts = []
            if d is not None: gk_parts.append(f"Δ={d:>6.3f}")
            if th is not None: gk_parts.append(f"θ={th:>7.1f}")
            if ga is not None: gk_parts.append(f"γ={ga:>8.5f}")
            if ve is not None: gk_parts.append(f"ν={ve:>6.1f}")
            if iv is not None: gk_parts.append(f"IV={iv:>5.1f}")
            gk_str = " ".join(gk_parts) if gk_parts else "—"
            fut_str = f"{ef:>8.1f}" if ef else "      —"
            print(f"  {i:<4} {side:<5} {et:>8} {xt:>8} {t['entry_price']:>8.2f} {peak:>8.2f} {t['exit_price']:>8.2f} "
                  f"{sign}{t['pnl']:>7.2f} {sign}{t['pct']:>6.1f}% {tag:<5} {pm_str:>35}  {fut_str} {gk_str}")


# ═══════════════════════════════════════════════════════════════
# INSTRUMENT PICKER — finds matching CE/PE/FUT combos
# ═══════════════════════════════════════════════════════════════

def find_combos(instruments):
    """Find all valid CE+PE+FUT combinations for backtesting.
    Matches CE+PE by underlying + strike + expiry.
    Picks the nearest future per underlying."""
    # Group by underlying
    by_underlying = defaultdict(lambda: {"CE": [], "PE": [], "FUT": []})
    for fpath, info in instruments.items():
        u = info["underlying"]
        k = info["kind"]
        if k in ("CE", "PE", "FUT"):
            by_underlying[u][k].append((fpath, info))

    combos = []
    for underlying, groups in by_underlying.items():
        if not groups["FUT"]:
            continue
        # Sort futures by expiry, use nearest
        futs = sorted(groups["FUT"], key=lambda x: x[1].get("expiry") or "")
        fut_path, fut_info = futs[0]

        # Build CE and PE lookup by (strike, expiry)
        ce_map = {}
        for path, info in groups["CE"]:
            if info["strike"]:
                key = (info["strike"], info.get("expiry"))
                ce_map[key] = (path, info)

        pe_map = {}
        for path, info in groups["PE"]:
            if info["strike"]:
                key = (info["strike"], info.get("expiry"))
                pe_map[key] = (path, info)

        # Find matching CE+PE pairs (same strike AND same expiry)
        common_keys = sorted(set(ce_map.keys()) & set(pe_map.keys()))

        for strike_expiry in common_keys:
            ce_path, ce_info = ce_map[strike_expiry]
            pe_path, pe_info = pe_map[strike_expiry]
            strike, expiry = strike_expiry

            # Try to find a future with matching expiry, fallback to nearest
            best_fut_path, best_fut_info = fut_path, fut_info
            for fp, fi in futs:
                if fi.get("expiry") == expiry:
                    best_fut_path, best_fut_info = fp, fi
                    break

            combos.append({
                "underlying": underlying,
                "strike": strike,
                "expiry": expiry,
                "ce_path": ce_path, "ce_info": ce_info,
                "pe_path": pe_path, "pe_info": pe_info,
                "fut_path": best_fut_path, "fut_info": best_fut_info,
            })

    return combos


# ═══════════════════════════════════════════════════════════════
# MAIN — interactive backtest runner
# ═══════════════════════════════════════════════════════════════

def _process_one_day(day_work):
    """Process ALL combos for one day in a single worker process."""
    import time
    t0 = time.time()
    date_str = day_work["date"]
    day_path = day_work["day_path"]
    args_dict = day_work["args"]
    combos = day_work["combos"]
    trail_tiers_str = args_dict.get("trail_tiers", "")
    
    # Reconstruct trail tiers
    if trail_tiers_str and isinstance(trail_tiers_str, str):
        trail_tiers = [(float(x.split(',')[0]), float(x.split(',')[1])) for x in trail_tiers_str.split(';')]
    else:
        trail_tiers = [(0,8),(10,6),(20,5),(30,4)]
    
    results = []
    for combo in combos:
        # Auto-detect fut_move_limit
        fml = args_dict.get("fut_move_limit", -1)
        if fml < 0:
            ul = combo.get("underlying", "").upper()
            fml = 200 if ("BANKNIFTY" in ul or "BANKEX" in ul) else 75
        
        # Determine per-underlying premium
        ul = combo.get("underlying", "").upper()
        if ("BANKNIFTY" in ul or "BANKEX" in ul):
            mn = args_dict.get("bnf_min_premium", 0) or args_dict.get("min_premium", 0)
            mx = args_dict.get("bnf_max_premium", 0) or args_dict.get("max_premium", 0)
        elif "BANK" not in ul:
            mn = args_dict.get("nif_min_premium", 0) or args_dict.get("min_premium", 0)
            mx = args_dict.get("nif_max_premium", 0) or args_dict.get("max_premium", 0)
        else:
            mn = args_dict.get("min_premium", 0)
            mx = args_dict.get("max_premium", 0)
        
        result = run_backtest(
            combo["ce_path"], combo["pe_path"], combo["fut_path"],
            combo["ce_info"], combo["pe_info"], combo["fut_info"],
            opt_low_margin=args_dict.get("opt_margin", 5),
            fut_low_margin=args_dict.get("fut_margin", 10),
            trail_tiers=trail_tiers,
            max_trades=args_dict.get("max_trades", 150),
            bev_trigger=args_dict.get("bev_trigger", 2.0),
            bev_buffer=args_dict.get("bev_buffer", 1.0),
            bev_buffer_mode=args_dict.get("bev_buffer_mode", "rupees"),
            straight_down_pct=args_dict.get("wide_stop", 25.0),
            start_time=args_dict.get("start_time", "09:15"),
            end_time=args_dict.get("end_time", "15:30"),
            fut_move_limit=fml,
            opt_open_rally_pct=args_dict.get("open_rally", 50),
            opt_atp_distance_pct=args_dict.get("atp_distance", 30),
            opt_buy_sell_ratio=args_dict.get("bs_ratio", 0.0),
            rally_base=args_dict.get("rally_base", "daylow_prevclose"),
            quiet=True, verbose=False,
            opt_momentum_threshold=args_dict.get("momentum_threshold", 1.0),
            first_only=args_dict.get("first_only", False),
            target_profit=args_dict.get("target_profit", 0),
            require_lower_wick=args_dict.get("require_lower_wick", False),
            block_marubozu=args_dict.get("block_marubozu", 0),
            max_hold=args_dict.get("max_hold", 0),
            max_range=args_dict.get("max_range", 0),
            max_momentum=args_dict.get("max_momentum", 0),
            max_above_high=args_dict.get("max_above_high", 0),
            rebuy_drop=args_dict.get("rebuy_drop", 0),
            min_premium=mn, max_premium=mx,
            side_filter=args_dict.get("side", "BOTH"),
            minute_interval=args_dict.get("minute_interval", 0),
        )
        if result:
            result["date"] = date_str
            result["strike"] = combo.get("strike", "")
            result["underlying"] = combo.get("underlying", "")
            results.append(result)
    
    elapsed = time.time() - t0
    return {"date": date_str, "results": results, "elapsed": elapsed, "combos_run": len(combos)}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Backtest Dual CE+PE Momentum Strategy")
    parser.add_argument("--data-dir", default="live_data", help="Base directory for harvested data")
    parser.add_argument("--date", help="Specific date to backtest (YYYY-MM-DD)")
    parser.add_argument("--underlying", help="Filter by underlying (BANKNIFTY, NIFTY, SENSEX)")
    parser.add_argument("--strike", type=int, help="Filter by specific strike price")
    parser.add_argument("--expiry", help="Filter by expiry date (YYYY-MM-DD)")
    parser.add_argument("--min-price", type=float, default=0, help="Min option entry price (default 0)")
    parser.add_argument("--max-price", type=float, default=99999, help="Max option entry price (default 99999)")
    parser.add_argument("--weekly-only", action="store_true", help="Only test weekly expiry (nearest to data date)")
    parser.add_argument("--list", action="store_true", help="List available data and exit")
    parser.add_argument("--all", action="store_true", help="Run all available combos on all dates")
    parser.add_argument("--opt-margin", type=int, default=5, help="Option day-low margin")
    parser.add_argument("--fut-margin", type=int, default=10, help="Future margin")
    parser.add_argument("--max-trades", type=int, default=150, help="Max trades per side")
    parser.add_argument("--bev-trigger", type=float, default=2.0, help="Breakeven trigger %% above entry (0=any increase, default 0)")
    parser.add_argument("--bev-buffer", type=float, default=1.0, help="Breakeven buffer value (default 1)")
    parser.add_argument("--bev-buffer-mode", type=str, default="rupees", choices=["rupees", "pct"],
                        help="BEV buffer mode: 'rupees' (entry+Rs1) or 'pct' (entry+1%%, default pct)")
    parser.add_argument("--wide-stop", type=float, default=25.0, help="Wide stop %% for straight-down trades (default 25.0)")
    parser.add_argument("--fut-move-limit", type=float, default=-1, help="Max future move from prev close (-1=auto: 200 BN, 75 NF/SX, (0=off, 200=BankNifty, 75=Nifty/Sensex)")
    parser.add_argument("--open-rally", type=float, default=50, help="Max option rally %% before blocking (0=off, default 50)")
    parser.add_argument("--rally-base", type=str, default="daylow_prevclose", choices=["open", "daylow_prevclose"],
                        help="Rally base: 'open' (today's open) or 'daylow_prevclose' (min of prev close & day low, default)")
    parser.add_argument("--atp-distance", type=float, default=30, help="Max distance above ATP %% before blocking (0=off, default 30)")
    parser.add_argument("--bs-ratio", type=float, default=0.0, help="Min buy/sell queue ratio (0=off, default 0)")
    parser.add_argument("--verbose", action="store_true", help="Print every condition check for every minute (very detailed)")
    parser.add_argument("--minute-interval", type=int, default=1, help="Check entry every N minutes (default 1, try 2 or 5)")
    parser.add_argument("--momentum-threshold", type=float, default=1.0, help="Option LTP must exceed prev avg by this Rs (default 1.0)")
    parser.add_argument("--atm-only", action="store_true", help="Only test ATM ± atm-range strikes (auto-detected from future price)")
    parser.add_argument("--atm-range", type=int, default=500, help="Strike range around ATM to test (default ±500)")
    parser.add_argument("--nearest-expiry", action="store_true", help="Keep only nearest expiry per underlying")
    parser.add_argument("--single-atm", action="store_true", help="Pick ONE combo per underlying (nearest expiry, closest strike)")
    parser.add_argument("--min-premium", type=float, default=0, help="Skip buy if option LTP < this Rs (0=off)")
    parser.add_argument("--max-premium", type=float, default=0, help="Skip buy if option LTP > this Rs (0=off)")
    parser.add_argument("--bnf-min-premium", type=float, default=0, help="BankNifty min premium (overrides --min-premium for BNF)")
    parser.add_argument("--bnf-max-premium", type=float, default=0, help="BankNifty max premium (overrides --max-premium for BNF)")
    parser.add_argument("--nif-min-premium", type=float, default=0, help="Nifty/Sensex min premium (overrides --min-premium for NIF/SEN)")
    parser.add_argument("--nif-max-premium", type=float, default=0, help="Nifty/Sensex max premium (overrides --max-premium for NIF/SEN)")
    parser.add_argument("--side", type=str, default="BOTH", choices=["CE","PE","BOTH"], help="Trade only CE, only PE, or BOTH (default BOTH)")
    parser.add_argument("--parallel", type=int, default=0, help="Run N combos in parallel (0=sequential, 4=recommended)")
    parser.add_argument("--first-only", action="store_true", help="Only take first trade per day per side, hold until --target-profit or EOD")
    parser.add_argument("--target-profit", type=float, default=0, help="Target profit %% to sell (0=use trail/stop as normal, 40=sell at +40%%)")
    parser.add_argument("--require-lower-wick", action="store_true", help="Only buy if prev minute candle has a lower wick (proven support)")
    parser.add_argument("--block-marubozu", type=float, default=0, help="Block entry if body/range > this (0=off, 0.80=block 80%%+ body candles)")
    parser.add_argument("--max-hold", type=int, default=0, help="Force exit after N minutes (0=off, 30=exit after 30min)")
    parser.add_argument("--max-range", type=float, default=0, help="Block entry if prev minute H-L > this Rs (0=off, CAUTION: high range is good)")
    parser.add_argument("--max-momentum", type=float, default=0, help="Block entry if LTP > avg + this Rs (0=off, 2=block if entry >avg+2)")
    parser.add_argument("--max-above-high", type=float, default=0, help="Block entry if LTP > prev minute High + this Rs (0=off, 2=block if entry >high+2)")
    parser.add_argument("--rebuy-drop", type=float, default=0, help="After a sell, next buy only after LTP drops this %% below prev entry (0=off, 10=require 10%% dip)")
    parser.add_argument("--trail-tiers", type=str, default="0:1,2:1,6:2,15:5",
                        help="Trail tiers as gain%%:trail%% pairs (default: 0:999,2:1,6:2,10:3,15:5)")
    parser.add_argument("--start-time", type=str, default="09:15", help="Start time HH:MM (default 09:15)")
    parser.add_argument("--end-time", type=str, default="15:30", help="End time HH:MM (default 15:30)")
    parser.add_argument("--no-trades", action="store_true", help="Hide individual trade details")
    parser.add_argument("--no-log", action="store_true", help="Don't save output to log file")
    # Strategy selection
    parser.add_argument("--strategy", type=str, default="momentum", choices=["momentum", "trend"],
                        help="Strategy to backtest: 'momentum' (default) or 'trend' (trend following)")
    # Trend-following specific args
    parser.add_argument("--ref-time", type=str, default="10:00", help="[trend] Reference time for future price (default 10:00)")
    parser.add_argument("--entry-time", type=str, default="10:30", help="[trend] Entry window opens (default 10:30)")
    parser.add_argument("--trend-exit-time", type=str, default="13:00", help="[trend] Time-based exit (default 13:00)")
    parser.add_argument("--breakout-pts", type=float, default=100, help="[trend] Future breakout points (default 100)")
    parser.add_argument("--stop-pct", type=float, default=30, help="[trend] Stop loss %% (default 30)")
    parser.add_argument("--target-pct", type=float, default=80, help="[trend] Profit target %% (default 80)")
    args = parser.parse_args()

    # Start logging
    log_path = None
    if not args.no_log:
        log_path = start_logging()

    print("\n" + "=" * 62)
    strat_name = "Dual CE+PE Momentum" if args.strategy == "momentum" else "Trend Following"
    print(f"  BACKTEST: {strat_name} Strategy")
    print("  Replay harvested tick data through strategy engine")
    print("=" * 62)
    # Log the command and timestamp
    import sys
    from datetime import datetime
    print(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Command: {' '.join(sys.argv)}")
    if log_path:
        print(f"  Log: {log_path}")

    # Parse trail tiers
    def parse_tiers(s):
        tiers = []
        for pair in s.split(","):
            pair = pair.strip()
            if ":" in pair:
                g, t = pair.split(":")
                tiers.append((float(g.strip()), float(t.strip())))
        tiers.sort(key=lambda x: x[0])
        return tiers

    trail_tiers = parse_tiers(args.trail_tiers)

    # Show current settings and allow interactive editing
    if not args.all and not args.list:
        print(f"\n  ── Strategy Settings ──")
        print(f"  Trail tiers: {', '.join(f'{g}:{t}' for g,t in trail_tiers)}")
        print(f"  BEV trigger: {args.bev_trigger}% │ BEV buffer: ₹{args.bev_buffer}")
        print(f"  Wide stop: {args.wide_stop}%")
        print(f"  Fut move limit: {args.fut_move_limit} pts (0=off)")
        print(f"  Open rally: {args.open_rally}% │ ATP dist: {args.atp_distance}% │ B/S ratio: {args.bs_ratio}")
        print(f"  Time: {args.start_time} → {args.end_time}")
        print(f"  Opt margin: {args.opt_margin} │ Fut margin: {args.fut_margin}")

        edit = input(f"\n  Edit settings? [n]: ").strip().lower()
        if edit == "y":
            print(f"\n  Enter trail tiers as gain%:trail%, comma-separated:")
            print(f"  Current: {', '.join(f'{g}:{t}' for g,t in trail_tiers)}")
            raw = input(f"  New tiers [enter to keep]: ").strip()
            if raw:
                trail_tiers = parse_tiers(raw)
                print(f"  ✓ Trail tiers: {', '.join(f'{g}:{t}' for g,t in trail_tiers)}")

            val = input(f"  BEV trigger % [{args.bev_trigger}]: ").strip()
            if val: args.bev_trigger = float(val)

            val = input(f"  BEV buffer ₹ [{args.bev_buffer}]: ").strip()
            if val: args.bev_buffer = float(val)

            val = input(f"  Wide stop % [{args.wide_stop}]: ").strip()
            if val: args.wide_stop = float(val)

            val = input(f"  Fut move limit [{args.fut_move_limit}]: ").strip()
            if val: args.fut_move_limit = float(val)

            val = input(f"  Open rally max % [{args.open_rally}]: ").strip()
            if val: args.open_rally = float(val)

            val = input(f"  ATP distance max % [{args.atp_distance}]: ").strip()
            if val: args.atp_distance = float(val)

            val = input(f"  Buy/Sell ratio min [{args.bs_ratio}]: ").strip()
            if val: args.bs_ratio = float(val)

            val = input(f"  Start time [{args.start_time}]: ").strip()
            if val: args.start_time = val

            val = input(f"  End time [{args.end_time}]: ").strip()
            if val: args.end_time = val

            print(f"\n  ✓ Final settings:")
            print(f"    Trail: {', '.join(f'{g}:{t}' for g,t in trail_tiers)}")
            print(f"    BEV: {args.bev_trigger}%/₹{args.bev_buffer} │ Wide: {args.wide_stop}% │ FutLimit: {args.fut_move_limit}")
            print(f"    Time: {args.start_time} → {args.end_time}")

    # Find available data
    dates = find_data_dirs(args.data_dir)
    if not dates:
        print(f"\n  ⚠ No data found in '{args.data_dir}/'")
        print(f"  Run auto_harvest.py or upstox_live.py first to collect data.")
        return

    print(f"\n  Found {len(dates)} date(s) with data:")
    for i, (d, path, n_files) in enumerate(dates, 1):
        print(f"    {i}. {d}  ({n_files} files)")

    if args.list:
        # Show detailed instrument list per date
        for d, path, _ in dates:
            print(f"\n  ── {d} ──")
            instruments = scan_instruments(path)
            combos = find_combos(instruments)
            if combos:
                for c in combos:
                    exp = c.get("expiry") or "?"
                    print(f"    {c['underlying']} {int(c['strike'])} CE+PE exp={exp} │ "
                          f"CE:{c['ce_info']['ticks']:,}t PE:{c['pe_info']['ticks']:,}t "
                          f"FUT:{c['fut_info']['ticks']:,}t")
            else:
                print(f"    No valid CE+PE+FUT combos found")
                for fp, info in instruments.items():
                    print(f"      {info['kind']:<5} {info['underlying']:<12} "
                          f"strike={info['strike'] or '—':<8} exp={info.get('expiry') or '—':<12} "
                          f"{info['ticks']:>8,} ticks  {info['fname']}")
        return

    # Select dates to process
    if args.date:
        selected_dates = [(d, p, n) for d, p, n in dates if d == args.date]
        if not selected_dates:
            print(f"\n  ⚠ No data for date '{args.date}'")
            return
    elif args.all:
        selected_dates = dates
    else:
        # Interactive selection
        pick = input(f"\n  Pick date [1-{len(dates)}, or 'all']: ").strip()
        if pick.lower() == "all":
            selected_dates = dates
        else:
            try:
                idx = int(pick) - 1
                selected_dates = [dates[idx]]
            except (ValueError, IndexError):
                selected_dates = [dates[0]]

    # Run backtests
    all_results = []

    _t_total_start = time.perf_counter()
    pending_days = []

    for date_str, day_path, _ in selected_dates:
        print(f"\n{'━' * 62}")
        print(f"  📅 {date_str}")
        print(f"{'━' * 62}")

        _t0 = time.perf_counter()
        instruments = scan_instruments(day_path)
        _t_scan = time.perf_counter() - _t0
        print(f"  ⏱ scan: {_t_scan:.1f}s", flush=True)
        combos = find_combos(instruments)

        if not combos:
            print(f"  ⚠ No valid CE+PE+FUT combinations found.")
            print(f"  Available instruments:")
            for fp, info in instruments.items():
                print(f"    {info['kind']:<5} {info['underlying']:<12} "
                      f"strike={info['strike'] or '—':<8} exp={info.get('expiry') or '—':<12} "
                      f"{info['ticks']:>8,} ticks  {os.path.relpath(fp, day_path)}")
            continue

        print(f"  Found {len(combos)} testable combo(s):")

        # ── Filter combos by CLI args ──
        if args.underlying:
            combos = [c for c in combos if args.underlying.upper() in c["underlying"].upper()]
        if args.strike:
            combos = [c for c in combos if int(c["strike"]) == args.strike]
        if args.expiry:
            combos = [c for c in combos if c.get("expiry","") == args.expiry]
        if args.weekly_only:
            from datetime import datetime
            data_dt = datetime.strptime(date_str, "%Y-%m-%d")
            expiry_dates = sorted(set(c.get("expiry","") for c in combos if c.get("expiry")))
            if expiry_dates:
                nearest = min(expiry_dates, key=lambda e: abs((datetime.strptime(e, "%Y-%m-%d") - data_dt).days))
                combos = [c for c in combos if c.get("expiry","") == nearest]
                print(f"  Weekly filter: using expiry {nearest}")

        # ── ATM-only filter: detect market level from future, filter strikes ──
        _t0 = time.perf_counter()
        if args.atm_only and combos:
            atm_range = args.atm_range
            # Detect ATM level per underlying (BN, NF, SX have different levels)
            underlying_levels = {}
            for c in combos:
                ul = c["underlying"]
                if ul in underlying_levels:
                    continue
                fp = c.get("fut_path")
                if fp and os.path.exists(fp):
                    try:
                        if fp.endswith(".parquet"):
                            import pyarrow.parquet as pq
                            pf = pq.ParquetFile(fp)
                            batch = pf.read_row_group(0, columns=["raw"])
                            for raw in batch.column("raw").to_pylist()[:20]:
                                try:
                                    rec = json.loads(raw)
                                    data = rec.get("data", rec)
                                    ltp = extract_ltp(data)
                                    if ltp and float(ltp) > 0:
                                        underlying_levels[ul] = float(ltp)
                                        break
                                except: continue
                        else:
                            with open(fp) as ff:
                                for fline in ff:
                                    fline = fline.strip()
                                    if not fline: continue
                                    try:
                                        rec = json.loads(fline)
                                        data = rec.get("data", rec)
                                        ltp = extract_ltp(data)
                                        if ltp and float(ltp) > 0:
                                            underlying_levels[ul] = float(ltp)
                                            break
                                    except: continue
                    except:
                        pass

            if underlying_levels:
                before = len(combos)
                filtered = []
                for c in combos:
                    ul = c["underlying"]
                    if ul in underlying_levels:
                        level = underlying_levels[ul]
                        if level - atm_range <= float(c["strike"]) <= level + atm_range:
                            filtered.append(c)
                    else:
                        filtered.append(c)  # keep if can't detect
                combos = filtered
                for ul, level in underlying_levels.items():
                    print(f"  ATM filter: {ul} ~{level:.0f}, strikes {level-atm_range:.0f}-{level+atm_range:.0f}")
                print(f"  ATM filter: {before} → {len(combos)} combos")
                for ul, lvl in sorted(underlying_levels.items()):
                    ul_count = sum(1 for c in combos if c.get("underlying") == ul)
                    print(f"    {ul}: future ~{lvl:.0f}, {ul_count} strikes kept")
            else:
                print(f"  ⚠ ATM filter: couldn't read any future prices, skipping filter")

        _t_atm = time.perf_counter() - _t0
        print(f"  ⏱ ATM + filters: {_t_atm:.1f}s", flush=True)

        # --nearest-expiry or --single-atm: keep only nearest expiry per underlying
        if getattr(args, 'nearest_expiry', False) or getattr(args, 'single_atm', False):
            by_ul = {}
            for c in combos:
                ul = c.get("underlying", "")
                if ul not in by_ul: by_ul[ul] = []
                by_ul[ul].append(c)
            nearest_combos = []
            for ul, ul_combos in by_ul.items():
                expiries = sorted(set(c.get("expiry", "9999") for c in ul_combos))
                nearest_exp = expiries[0]
                if nearest_exp == date_str and len(expiries) > 1:
                    print(f"  ⚠ {ul}: expiry day {nearest_exp}, using next: {expiries[1]}")
                    nearest_exp = expiries[1]
                filtered = [c for c in ul_combos if c.get("expiry") == nearest_exp]
                nearest_combos.extend(filtered)
                print(f"  Nearest expiry: {ul} → {nearest_exp} ({len(filtered)} strikes)")
            before_ne = len(combos)
            combos = nearest_combos
            print(f"  Nearest expiry: {before_ne} → {len(combos)} combos")

        # --single-atm: pick ONE combo per underlying (closest strike to ATM)
        if getattr(args, 'single_atm', False) and underlying_levels:
            by_ul2 = {}
            for c in combos:
                ul = c.get("underlying", "")
                if ul not in by_ul2: by_ul2[ul] = []
                by_ul2[ul].append(c)
            best_combos = []
            for ul, ul_combos in by_ul2.items():
                atm = underlying_levels.get(ul, 0)
                best = min(ul_combos, key=lambda c: abs(float(c.get("strike", 0)) - atm))
                best_combos.append(best)
                print(f"  Single ATM: {ul} → strike {best['strike']} (fut ~{atm:.0f})")
            before_sa = len(combos)
            combos = best_combos
            print(f"  Single ATM: {before_sa} → {len(combos)} combos")


        def _get_premium(combo, args, which):
            """Get min/max premium for this combo's underlying."""
            ul = combo.get("underlying", "").upper()
            if which == 'min':
                if ("BANKNIFTY" in ul or "BANKEX" in ul) and getattr(args, 'bnf_min_premium', 0) > 0:
                    return args.bnf_min_premium
                if ("NIFTY" in ul or "SENSEX" in ul) and "BANK" not in ul and getattr(args, 'nif_min_premium', 0) > 0:
                    return args.nif_min_premium
                return getattr(args, 'min_premium', 0)
            else:
                if ("BANKNIFTY" in ul or "BANKEX" in ul) and getattr(args, 'bnf_max_premium', 0) > 0:
                    return args.bnf_max_premium
                if ("NIFTY" in ul or "SENSEX" in ul) and "BANK" not in ul and getattr(args, 'nif_max_premium', 0) > 0:
                    return args.nif_max_premium
                return getattr(args, 'max_premium', 0)

        # Premium pre-filter: skip combos where option LTP is way outside range
        if combos and (getattr(args, 'bnf_min_premium', 0) > 0 or getattr(args, 'nif_min_premium', 0) > 0
                      or getattr(args, 'min_premium', 0) > 0 or getattr(args, 'max_premium', 0) > 0):
            before_pf = len(combos)
            kept = []
            skipped = 0
            side = getattr(args, 'side', 'BOTH')
            for c in combos:
                mn = _get_premium(c, args, 'min')
                mx = _get_premium(c, args, 'max')
                if mn <= 0 and mx <= 0:
                    kept.append(c)
                    continue
                paths = []
                if side == "PE": paths = [c.get("pe_path")]
                elif side == "CE": paths = [c.get("ce_path")]
                else: paths = [c.get("ce_path"), c.get("pe_path")]
                any_ok = False
                checked = False
                for p in paths:
                    if not p: continue
                    try:
                        if p.endswith(".parquet"):
                            import pyarrow.parquet as pq
                            pf = pq.ParquetFile(p)
                            batch = pf.read_row_group(0, columns=["raw"])
                            for raw in batch.column("raw").to_pylist()[:20]:
                                rec = json.loads(raw)
                                ltp = extract_ltp(rec.get("data", rec))
                                if ltp and float(ltp) > 0:
                                    checked = True
                                    if not (mx > 0 and float(ltp) > mx * 1.1) and not (mn > 0 and float(ltp) < mn * 0.9):
                                        any_ok = True
                                    break
                        else:
                            with open(p) as ff:
                                for fline in ff:
                                    fline = fline.strip()
                                    if not fline: continue
                                    rec = json.loads(fline)
                                    ltp = extract_ltp(rec.get("data", rec))
                                    if ltp and float(ltp) > 0:
                                        checked = True
                                        if not (mx > 0 and float(ltp) > mx * 1.1) and not (mn > 0 and float(ltp) < mn * 0.9):
                                            any_ok = True
                                        break
                    except: pass
                if not checked or any_ok:
                    kept.append(c)
                else:
                    skipped += 1
            combos = kept
            if skipped > 0:
                print(f"  Premium pre-filter: skipped {skipped}, {len(combos)} remaining")
        if args.min_price > 0 or args.max_price < 99999:
            print(f"  Price filter: ₹{args.min_price}-{args.max_price} (applied during trade)")

        if not combos:
            print(f"  ⚠ No combos match filters. Skipping.")
            continue
        if len(combos) < 459:
            print(f"  After filters: {len(combos)} combo(s)")

        for i, c in enumerate(combos, 1):
            exp = c.get("expiry") or "?"
            ce_t = c["ce_info"]["ticks"]
            pe_t = c["pe_info"]["ticks"]
            fut_t = c["fut_info"]["ticks"]
            print(f"    {i}. {c['underlying']} {int(c['strike'])} CE+PE exp={exp} "
                  f"│ CE:{ce_t:,}t PE:{pe_t:,}t FUT:{fut_t:,}t")

        # Select combos
        if len(combos) == 1 or args.all:
            selected_combos = combos
        else:
            cpick = input(f"  Pick combo [1-{len(combos)}, or 'all']: ").strip()
            if cpick.lower() == "all":
                selected_combos = combos
            else:
                try:
                    selected_combos = [combos[int(cpick) - 1]]
                except (ValueError, IndexError):
                    selected_combos = combos


        def _run_one_combo(combo):
            """Run backtest on a single combo. Returns result dict or None."""
            fml = args.fut_move_limit
            if fml < 0:
                underlying = combo.get("underlying", "").upper()
                if "BANKNIFTY" in underlying or "BANKEX" in underlying:
                    fml = 200
                else:
                    fml = 75
            if args.strategy == "trend":
                return run_trend_backtest(
                    combo["ce_path"], combo["pe_path"], combo["fut_path"],
                    combo["ce_info"], combo["pe_info"], combo["fut_info"],
                    ref_time=args.ref_time,
                    entry_time=args.entry_time,
                    exit_time=args.trend_exit_time,
                    breakout_pts=args.breakout_pts,
                    stop_pct=args.stop_pct,
                    target_pct=args.target_pct,
                    quiet=(args.parallel > 0),
                )
            else:
                return run_backtest(
                    combo["ce_path"], combo["pe_path"], combo["fut_path"],
                    combo["ce_info"], combo["pe_info"], combo["fut_info"],
                    opt_low_margin=args.opt_margin,
                    fut_low_margin=args.fut_margin,
                    trail_tiers=trail_tiers,
                    max_trades=args.max_trades,
                    bev_trigger=args.bev_trigger,
                    bev_buffer=args.bev_buffer,
                    bev_buffer_mode=args.bev_buffer_mode,
                    straight_down_pct=args.wide_stop,
                    start_time=args.start_time,
                    end_time=args.end_time,
                    fut_move_limit=fml,
                    opt_open_rally_pct=args.open_rally,
                    opt_atp_distance_pct=args.atp_distance,
                    opt_buy_sell_ratio=args.bs_ratio,
                    rally_base=args.rally_base,
                    verbose=args.verbose,
                    minute_interval=args.minute_interval,
                    opt_momentum_threshold=args.momentum_threshold,
                    first_only=args.first_only,
                    target_profit=args.target_profit,
                    require_lower_wick=args.require_lower_wick,
                    block_marubozu=args.block_marubozu,
                    max_hold=args.max_hold,
                    max_range=args.max_range,
                    max_momentum=args.max_momentum,
                    max_above_high=args.max_above_high,
                    rebuy_drop=args.rebuy_drop,
                    min_premium=_get_premium(combo, args, 'min'),
                    max_premium=_get_premium(combo, args, 'max'),
                    side_filter=getattr(args, 'side', 'BOTH'),
                    quiet=(args.parallel > 0),
                )

        if args.parallel > 0 and len(selected_combos) > 1 and not args.verbose:
            _quiet_mode = True
            # Queue this day for parallel execution later
            trail_str = ";".join(f"{a},{b}" for a,b in trail_tiers) if trail_tiers else ""
            day_work = {
                "date": date_str, "day_path": day_path,
                "combos": selected_combos,
                "args": {
                    "opt_margin": args.opt_margin, "fut_margin": args.fut_margin,
                    "trail_tiers": trail_str, "max_trades": args.max_trades,
                    "bev_trigger": args.bev_trigger, "bev_buffer": args.bev_buffer,
                    "bev_buffer_mode": args.bev_buffer_mode, "wide_stop": args.wide_stop,
                    "start_time": args.start_time, "end_time": args.end_time,
                    "fut_move_limit": args.fut_move_limit,
                    "open_rally": args.open_rally, "atp_distance": args.atp_distance,
                    "bs_ratio": args.bs_ratio, "rally_base": args.rally_base,
                    "momentum_threshold": args.momentum_threshold,
                    "first_only": args.first_only, "target_profit": args.target_profit,
                    "require_lower_wick": args.require_lower_wick,
                    "block_marubozu": args.block_marubozu, "max_hold": args.max_hold,
                    "max_range": args.max_range, "max_momentum": args.max_momentum,
                    "max_above_high": args.max_above_high, "rebuy_drop": args.rebuy_drop,
                    "min_premium": getattr(args, 'min_premium', 0),
                    "max_premium": getattr(args, 'max_premium', 0),
                    "bnf_min_premium": getattr(args, 'bnf_min_premium', 0),
                    "bnf_max_premium": getattr(args, 'bnf_max_premium', 0),
                    "nif_min_premium": getattr(args, 'nif_min_premium', 0),
                    "nif_max_premium": getattr(args, 'nif_max_premium', 0),
                    "side": getattr(args, 'side', 'BOTH'),
                    "minute_interval": getattr(args, 'minute_interval', 0),
                    "strategy": getattr(args, 'strategy', 'momentum'),
                },
            }
            pending_days.append(day_work)
            print(f"  Queued {len(selected_combos)} combos for parallel", flush=True)
        else:
            # Sequential execution
            clear_tick_cache()
            day_results = []
            _t_day_start = time.perf_counter()
            # Track which FUT files to keep in cache (shared across combos)
            _fut_paths = set(c.get("fut_path","") for c in selected_combos)
            for ci, combo in enumerate(selected_combos, 1):
                # Clear CE/PE from cache between combos (keep FUT — shared)
                if ci > 1:
                    stale = [k for k in _tick_cache if k not in _fut_paths]
                    for k in stale:
                        del _tick_cache[k]
                print(f"\n  [{ci}/{len(selected_combos)}] {combo.get('underlying','')} {combo.get('strike','')} exp={combo.get('expiry','')}", flush=True)
                _t_combo = time.perf_counter()
                result = _run_one_combo(combo)
                _t_combo_done = time.perf_counter() - _t_combo
                print(f"    ⏱ {_t_combo_done:.1f}s", flush=True)
                if result:
                    result["date"] = date_str
                    result["strike"] = combo["strike"]
                    result["underlying"] = combo["underlying"]
                    all_results.append(result)
                    day_results.append(result)
                    print_result(result, show_trades=not args.no_trades)
            # Day summary
            if day_results:
                day_trades = sum(r.get("ce_count",0) + r.get("pe_count",0) for r in day_results)
                day_wins = sum(r.get("ce_wins",0) + r.get("pe_wins",0) for r in day_results)
                day_pnl = sum(r.get("total_pnl",0) for r in day_results)
                day_mark = "✓" if day_pnl > 0 else "✗"
                print(f"\n  {'━'*50}")
                print(f"  📊 DAY TOTAL {date_str}: {day_trades} trades, {day_wins} wins, {day_mark} Rs{day_pnl:+,.2f}")
                _t_day_elapsed = time.perf_counter() - _t_day_start
                print(f"  ⏱ Day: {_t_day_elapsed:.1f}s total, {_t_day_elapsed/max(len(selected_combos),1):.1f}s per combo")
                print(f"  {'━'*50}")


    # Execute queued days in parallel (ProcessPool — true multi-core)
    if pending_days:
        from concurrent.futures import ProcessPoolExecutor, as_completed
        n_workers = min(args.parallel, len(pending_days))
        print(f"\n  Running {len(pending_days)} days in parallel ({n_workers} processes)...", flush=True)
        
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            futures = {pool.submit(_process_one_day, dw): dw["date"] for dw in pending_days}
            for future in as_completed(futures):
                day_date = futures[future]
                try:
                    day_result = future.result()
                    n_results = len(day_result["results"])
                    elapsed = day_result["elapsed"]
                    print(f"  ✓ {day_date}: {day_result['combos_run']} combos, {n_results} with trades, {elapsed:.0f}s", flush=True)
                    day_pnl = 0
                    day_trades = 0
                    day_wins = 0
                    for result in day_result["results"]:
                        all_results.append(result)
                        print_result(result, show_trades=not args.no_trades)
                        day_trades += result.get("ce_count",0) + result.get("pe_count",0)
                        day_wins += result.get("ce_wins",0) + result.get("pe_wins",0)
                        day_pnl += result.get("total_pnl",0)
                    if day_result["results"]:
                        day_mark = "✓" if day_pnl > 0 else "✗"
                        print(f"\n  {'━'*50}")
                        print(f"  📊 DAY TOTAL {day_date}: {day_trades} trades, {day_wins} wins, {day_mark} Rs{day_pnl:+,.2f}")
                        print(f"  {'━'*50}")
                except Exception as e:
                    print(f"  ⚠ {day_date}: Error — {e}", flush=True)

    _t_total_end = time.perf_counter()
    print(f"\n  ⏱ TOTAL RUN TIME: {_t_total_end - _t_total_start:.0f}s ({(_t_total_end - _t_total_start)/60:.1f} min)", flush=True)

    # Multi-date summary
    if len(all_results) > 1:
        print(f"\n{'═' * 62}")
        print(f"  MULTI-DAY SUMMARY")
        print(f"{'═' * 62}")
        print(f"\n  {'Date':<12} {'Instrument':<20} {'Trades':>7} {'Wins':>6} {'P&L':>10}")
        print(f"  {'─' * 58}")

        grand_pnl = 0
        grand_trades = 0
        grand_wins = 0

        for r in all_results:
            total = r["ce_count"] + r["pe_count"]
            wins = r["ce_wins"] + r["pe_wins"]
            sign = "+" if r["total_pnl"] >= 0 else ""
            inst = f"{r['underlying']} {int(r['strike'])}"
            print(f"  {r['date']:<12} {inst:<20} {total:>7} {wins:>6} {sign}₹{r['total_pnl']:>8.2f}")
            grand_pnl += r["total_pnl"]
            grand_trades += total
            grand_wins += wins

        print(f"  {'─' * 58}")
        gs = "+" if grand_pnl >= 0 else ""
        wr = (grand_wins / grand_trades * 100) if grand_trades > 0 else 0
        print(f"  {'TOTAL':<12} {'':>20} {grand_trades:>7} {grand_wins:>6} {gs}₹{grand_pnl:>8.2f}")
        print(f"  {'Win rate':<12} {'':>20} {'':>7} {wr:>5.1f}%")
        print(f"  {'═' * 62}")


if __name__ == "__main__":
    try:
        main()
    finally:
        stop_logging()
