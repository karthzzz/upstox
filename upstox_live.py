"""
Upstox Live Data Collector
==========================
pip install upstox-python-sdk pandas
Run:  python upstox_live.py
"""
import json
import os
import sys
import base64
import threading
import time
from datetime import datetime, date, timezone, timedelta
IST = timezone(timedelta(hours=5, minutes=30))
import upstox_client
from upstox_client.rest import ApiException
# ═══════════════════════════════════════════════════════════════
# TEE LOGGER — mirrors all terminal output to a timestamped file
# ═══════════════════════════════════════════════════════════════
LOG_DIR = "run_logs"
class TeeLogger:
    """Writes to both terminal and a log file simultaneously.
    Every print() and input() prompt is captured."""
    def __init__(self, log_path):
        self.terminal = sys.stdout
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        self.log_file = open(log_path, "a", buffering=1, encoding="utf-8")
        self.log_path = log_path
    def write(self, message):
        self.terminal.write(message)
        clean = self._strip_ansi(message)
        self.log_file.write(clean)
    def flush(self):
        self.terminal.flush()
        self.log_file.flush()
    def close(self):
        self.log_file.close()
    @staticmethod
    def _strip_ansi(text):
        import re
        return re.sub(r'\033\[[0-9;]*m', '', text)
    def isatty(self):
        return self.terminal.isatty()
    @property
    def encoding(self):
        return self.terminal.encoding
def start_logging():
    now = datetime.now(IST)
    filename = f"run_{now.strftime('%Y-%m-%d_%H-%M-%S')}.log"
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
# CONFIG
# ═══════════════════════════════════════════════════════════════
ACCESS_TOKEN = ""   # paste your analytics token here, or enter at prompt
MODE = "full_d30"
SAVE_DIR = "live_data"
INDICES = {
    "banknifty":  "NSE_INDEX|Nifty Bank",
    "nifty":      "NSE_INDEX|Nifty 50",
    "finnifty":   "NSE_INDEX|Nifty Fin Service",
    "sensex":     "BSE_INDEX|SENSEX",
    "bankex":     "BSE_INDEX|BANKEX",
    "midcpnifty": "NSE_INDEX|NIFTY MID SELECT",
    "indiavix":   "NSE_INDEX|India VIX",
}
MCX_COMMODITIES = {
    "crudeoil":   "CRUDE OIL",
    "gold":       "GOLD",
    "silver":     "SILVER",
    "naturalgas": "NATURAL GAS",
    "copper":     "COPPER",
}
CURRENCY_SEARCH = {
    "usdinr":     "USDINR",
    "eurinr":     "EURINR",
    "gbpinr":     "GBPINR",
}
# ═══════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════
def decode_jwt(token):
    try:
        payload = token.split(".")[1]
        payload += "=" * (4 - len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload))
    except Exception:
        return None
def show_account_info(token):
    claims = decode_jwt(token) or {}
    user_id = claims.get("sub", "?")
    is_plus = claims.get("isPlusPlan", False)
    is_extended = claims.get("isExtended", False)
    iat = claims.get("iat")
    exp = claims.get("exp")
    issued = datetime.fromtimestamp(iat).strftime("%d %b %Y") if iat else "?"
    expires = datetime.fromtimestamp(exp).strftime("%d %b %Y") if exp else "?"
    w = 40
    print(f"\n  ┌{'─' * w}┐")
    print(f"  │  {'👤 ' + user_id:<{w-2}}│")
    print(f"  │{'─' * w}│")
    print(f"  │  {'Plus Plan: ' + ('✓ YES' if is_plus else '✗ NO'):<{w-2}}│")
    print(f"  │  {'Token: ' + ('Analytics — 1 year' if is_extended else 'Daily OAuth'):<{w-2}}│")
    print(f"  │  {'Issued: ' + issued:<{w-2}}│")
    print(f"  │  {'Expires: ' + expires:<{w-2}}│")
    print(f"  └{'─' * w}┘")
    if not is_plus and MODE == "full_d30":
        print(f"\n  ⚠ full_d30 needs Plus Plan. Switching MODE to 'full'.")
        return "downgrade"
    return True
# ═══════════════════════════════════════════════════════════════
# SDK CLIENT + LTP
# ═══════════════════════════════════════════════════════════════
def make_client(token):
    config = upstox_client.Configuration()
    config.access_token = token
    return upstox_client.ApiClient(config)
def fetch_ltp(client, instrument_keys):
    if not instrument_keys:
        return {}
    result = {}
    keys_str = ",".join(instrument_keys)
    try:
        api = upstox_client.MarketQuoteV3Api(client)
        resp = api.get_ltp(instrument_key=keys_str)
        data = resp.data if hasattr(resp, "data") else resp.get("data", {})
        if data:
            for key, val in (data.items() if isinstance(data, dict) else []):
                d = val.to_dict() if hasattr(val, "to_dict") else val
                ltp = d.get("last_price") or d.get("ltp")
                if ltp:
                    result[key] = ltp
        if result:
            return result
    except Exception:
        pass
    try:
        api = upstox_client.MarketQuoteApi(client)
        resp = api.ltp(keys_str, "2.0")
        data = resp.data if hasattr(resp, "data") else resp.get("data", {})
        if data:
            for key, val in (data.items() if isinstance(data, dict) else []):
                d = val.to_dict() if hasattr(val, "to_dict") else val
                ltp = d.get("last_price") or d.get("ltp")
                if ltp:
                    result[key] = ltp
        if result:
            return result
    except Exception:
        pass
    return result
def validate_token(client):
    try:
        api = upstox_client.MarketQuoteV3Api(client)
        resp = api.get_ltp(instrument_key="NSE_INDEX|Nifty 50")
        data = resp.data if hasattr(resp, "data") else resp.get("data", {})
        if data:
            for key, val in (data.items() if isinstance(data, dict) else []):
                d = val.to_dict() if hasattr(val, "to_dict") else val
                ltp = d.get("last_price") or d.get("ltp")
                if ltp:
                    print(f"\n  ✓ Token valid! Nifty 50 LTP: ₹{ltp}")
                    return True
        print(f"\n  ✓ Token valid (API responded)")
        return True
    except Exception as e:
        try:
            api = upstox_client.MarketQuoteApi(client)
            resp = api.ltp("NSE_INDEX|Nifty 50", "2.0")
            print(f"\n  ✓ Token valid (V2 API)")
            return True
        except Exception as e2:
            print(f"\n  ✗ Token validation failed: {e2}")
            return False
# ═══════════════════════════════════════════════════════════════
# INSTRUMENT SEARCH
# ═══════════════════════════════════════════════════════════════
def search_instrument(client, query):
    api = upstox_client.InstrumentsApi(client)
    try:
        resp = api.search_instrument(query)
        data = resp.data if hasattr(resp, "data") else []
        return [r.to_dict() if hasattr(r, "to_dict") else r for r in data]
    except ApiException as e:
        print(f"  Search error: {e.body if hasattr(e, 'body') else e}")
        return []
# ═══════════════════════════════════════════════════════════════
# OPTION CHAIN
# ═══════════════════════════════════════════════════════════════
def clean_expiry(expiry_val):
    """Normalize expiry to YYYY-MM-DD string. Handles epoch ms, epoch s, and date strings."""
    if expiry_val is None:
        return None
    if isinstance(expiry_val, (int, float)):
        if expiry_val > 1_000_000_000_000:
            return datetime.fromtimestamp(expiry_val / 1000, tz=IST).strftime("%Y-%m-%d")
        elif expiry_val > 1_000_000_000:
            return datetime.fromtimestamp(expiry_val, tz=IST).strftime("%Y-%m-%d")
        return str(int(expiry_val))
    s = str(expiry_val).strip()
    try:
        num = float(s)
        if num > 1_000_000_000_000:
            return datetime.fromtimestamp(num / 1000, tz=IST).strftime("%Y-%m-%d")
        elif num > 1_000_000_000:
            return datetime.fromtimestamp(num, tz=IST).strftime("%Y-%m-%d")
    except ValueError:
        pass
    if " " in s:
        s = s.split(" ")[0]
    if "T" in s:
        s = s.split("T")[0]
    return s
def get_expiries(client, underlying_key):
    api = upstox_client.OptionsApi(client)
    try:
        resp = api.get_option_contracts(underlying_key)
        data = resp.data if hasattr(resp, "data") else []
        expiries = set()
        for c in data:
            d = c.to_dict() if hasattr(c, "to_dict") else c
            exp = d.get("expiry")
            if exp:
                expiries.add(clean_expiry(exp))
        return sorted(expiries)
    except ApiException as e:
        print(f"  Error fetching expiries: {e.body if hasattr(e, 'body') else e}")
        return []
def get_option_chain(client, underlying_key, expiry):
    api = upstox_client.OptionsApi(client)
    expiry = clean_expiry(expiry)
    try:
        resp = api.get_put_call_option_chain(underlying_key, expiry)
        data = resp.data if hasattr(resp, "data") else []
        chain = []
        for entry in data:
            e = entry.to_dict() if hasattr(entry, "to_dict") else entry
            call = e.get("call_options") or {}
            put = e.get("put_options") or {}
            cm = call.get("market_data") or {}
            pm = put.get("market_data") or {}
            cg = call.get("option_greeks") or {}
            pg = put.get("option_greeks") or {}
            chain.append({
                "strike":     e.get("strike_price"),
                "spot":       e.get("underlying_spot_price"),
                "pcr":        e.get("pcr"),
                "call_ltp":   cm.get("ltp"),
                "call_iv":    cg.get("iv"),
                "call_delta": cg.get("delta"),
                "call_oi":    cm.get("oi"),
                "call_key":   call.get("instrument_key"),
                "put_ltp":    pm.get("ltp"),
                "put_iv":     pg.get("iv"),
                "put_delta":  pg.get("delta"),
                "put_oi":     pm.get("oi"),
                "put_key":    put.get("instrument_key"),
            })
        chain.sort(key=lambda x: x["strike"] or 0)
        return chain
    except ApiException as e:
        print(f"  Error fetching chain: {e.body if hasattr(e, 'body') else e}")
        return []
def display_chain(chain, spot):
    if not chain:
        print("  No chain data.")
        return
    atm_idx = min(range(len(chain)), key=lambda i: abs((chain[i]["strike"] or 0) - spot))
    print(f"\n  {'#':>4} {'STRIKE':>10} │ {'C_LTP':>8} {'C_IV':>7} {'C_Δ':>7} {'C_OI':>10} │ {'P_LTP':>8} {'P_IV':>7} {'P_Δ':>7} {'P_OI':>10}")
    print(f"  {'─'*4} {'─'*10} │ {'─'*8} {'─'*7} {'─'*7} {'─'*10} │ {'─'*8} {'─'*7} {'─'*7} {'─'*10}")
    def fmt(val, d=2):
        return "-" if val is None else (f"{int(val)}" if d == 0 else f"{val:.{d}f}")
    for i, s in enumerate(chain):
        marker = "ATM>" if i == atm_idx else f"{i+1:>4}"
        strike = s["strike"] or 0
        print(
            f"  {marker} {strike:>10.1f} │ "
            f"{fmt(s.get('call_ltp')):>8} {fmt(s.get('call_iv'), 1):>7} "
            f"{fmt(s.get('call_delta'), 3):>7} {fmt(s.get('call_oi'), 0):>10} │ "
            f"{fmt(s.get('put_ltp')):>8} {fmt(s.get('put_iv'), 1):>7} "
            f"{fmt(s.get('put_delta'), 3):>7} {fmt(s.get('put_oi'), 0):>10}"
        )
def pick_options_from_chain(chain, spot):
    display_chain(chain, spot)
    print(f"\n  Spot: {spot}")
    print(f"\n  How to pick:")
    print(f"    • Strike prices:  51000CE, 51000PE, 51200CE")
    print(f"    • Row numbers:    3CE, 3PE, 5CE")
    print(f"    • 'atm'         → ATM CE + PE")
    print(f"    • 'all N'       → ATM ± N strikes CE + PE  (e.g. 'all 3')")
    print(f"    • Comma-separated, mix and match")
    raw = input("\n  Your picks: ").strip()
    if not raw:
        return []
    atm_idx = min(range(len(chain)), key=lambda i: abs((chain[i]["strike"] or 0) - spot))
    selected = []
    for part in raw.split(","):
        part = part.strip().upper()
        if part == "ATM":
            s = chain[atm_idx]
            if s.get("call_key"):
                selected.append((s["call_key"], "CE", s))
            if s.get("put_key"):
                selected.append((s["put_key"], "PE", s))
            continue
        if part.startswith("ALL"):
            try:
                n = int(part.replace("ALL", "").strip() or "3")
            except ValueError:
                n = 3
            for i in range(max(0, atm_idx - n), min(len(chain), atm_idx + n + 1)):
                if chain[i].get("call_key"):
                    selected.append((chain[i]["call_key"], "CE", chain[i]))
                if chain[i].get("put_key"):
                    selected.append((chain[i]["put_key"], "PE", chain[i]))
            continue
        side = None
        num_str = part
        if part.endswith("CE"):
            side = "CE"
            num_str = part[:-2].strip()
        elif part.endswith("PE"):
            side = "PE"
            num_str = part[:-2].strip()
        try:
            num = float(num_str)
        except ValueError:
            print(f"  [?] Can't parse: {part}")
            continue
        row = None
        strike_match = [s for s in chain if s["strike"] == num]
        if strike_match:
            row = strike_match[0]
        elif 1 <= num <= len(chain) and num == int(num):
            row = chain[int(num) - 1]
        else:
            print(f"  [?] Strike {num} not found")
            continue
        if row:
            if side == "CE" or side is None:
                if row.get("call_key"):
                    selected.append((row["call_key"], "CE", row))
            if side == "PE" or side is None:
                if row.get("put_key"):
                    selected.append((row["put_key"], "PE", row))
    seen = set()
    unique = []
    for key, side, row in selected:
        if key and key not in seen:
            seen.add(key)
            unique.append((key, side, row))
    return unique
# ═══════════════════════════════════════════════════════════════
# FILE WRITER
# ═══════════════════════════════════════════════════════════════
class InstrumentWriter:
    def __init__(self, save_dir, sub_meta=None):
        today = date.today().isoformat()
        self.dir = os.path.join(save_dir, today)
        os.makedirs(self.dir, exist_ok=True)
        self._sub_meta = sub_meta or {}
        self._files = {}
        self._counts = {}
        self._lock = threading.Lock()
    def _get_file(self, instrument_key):
        if instrument_key not in self._files:
            meta = self._sub_meta.get(instrument_key, {})
            name_part = meta.get("name", "").replace(" ", "_").replace("|", "_")
            key_part = instrument_key.replace("|", "_").replace(" ", "_")
            if name_part and name_part != key_part:
                fname = f"{name_part}_{key_part}.jsonl"
            else:
                fname = f"{key_part}.jsonl"
            path = os.path.join(self.dir, fname)
            self._files[instrument_key] = open(path, "a", buffering=1)
            self._counts[instrument_key] = 0
        return self._files[instrument_key]
    def write(self, instrument_key, raw_data, timestamp):
        record = {"ts": timestamp, "key": instrument_key, "data": raw_data}
        with self._lock:
            f = self._get_file(instrument_key)
            f.write(json.dumps(record, default=str) + "\n")
            self._counts[instrument_key] = self._counts.get(instrument_key, 0) + 1
    def close(self):
        with self._lock:
            for f in self._files.values():
                f.close()
            self._files.clear()
    @property
    def stats(self):
        with self._lock:
            return dict(self._counts)
    @property
    def total_ticks(self):
        with self._lock:
            return sum(self._counts.values())
# ═══════════════════════════════════════════════════════════════
# LIVE STREAMING
# ═══════════════════════════════════════════════════════════════
def stream(client, instrument_keys, mode, sub_meta=None, strategy_runner=None):
    if not instrument_keys:
        print("  No instruments to stream.")
        return
    sub_meta = sub_meta or {}
    writer = InstrumentWriter(SAVE_DIR, sub_meta)
    COLORS = ["\033[96m","\033[93m","\033[92m","\033[95m","\033[94m","\033[91m","\033[97m","\033[36m"]
    RESET = "\033[0m"
    key_colors = {}
    key_names = {}
    for i, k in enumerate(instrument_keys):
        key_colors[k] = COLORS[i % len(COLORS)]
        meta = sub_meta.get(k, {})
        key_names[k] = meta.get("name", k.split("|")[-1] if "|" in k else k)
    print(f"\n  ┌──────────────────────────────────────────────────┐")
    print(f"  │  Streaming {len(instrument_keys):>3} instruments in '{mode}' mode        │")
    print(f"  │  Saving to: {writer.dir:<37}│")
    print(f"  │  Press Ctrl+C to stop                            │")
    print(f"  └──────────────────────────────────────────────────┘\n")
    for k in instrument_keys:
        c = key_colors[k]
        strat_marker = ""
        if strategy_runner:
            if isinstance(strategy_runner, DualStrategyRunner):
                if k == strategy_runner.call_key:
                    strat_marker = " ◀ CE STRATEGY"
                elif k == strategy_runner.put_key:
                    strat_marker = " ◀ PE STRATEGY"
                elif k == strategy_runner.future_key:
                    strat_marker = " ◀ FUTURE"
            else:
                if k == strategy_runner.instrument_key:
                    strat_marker = " ◀ STRATEGY"
                elif k == strategy_runner.future_key:
                    strat_marker = " ◀ FUTURE"
        print(f"    {c}● {key_names[k]:<25}{RESET} {k}{strat_marker}")
    if strategy_runner:
        fut_name = key_names.get(strategy_runner.future_key, "") if strategy_runner.future_key else "none"
        print(f"\n  ┌──────────────────────────────────────────────────┐")
        if isinstance(strategy_runner, DualStrategyRunner):
            print(f"  │  Strategy: Dual CE+PE Momentum                  │")
            ce_name = strategy_runner.call_trader.instrument_name
            pe_name = strategy_runner.put_trader.instrument_name
            print(f"  │  CE:       {ce_name:<38}│")
            print(f"  │  PE:       {pe_name:<38}│")
        else:
            print(f"  │  Strategy: {strategy_runner.strategy.name:<38}│")
            print(f"  │  Trading:  {strategy_runner.trader.instrument_name:<38}│")
        if strategy_runner.future_key:
            print(f"  │  Future:   {fut_name:<38}│")
        print(f"  │  Lot size: {strategy_runner.trader.lot_size:<5} │ Max trades: {strategy_runner.trader.max_trades:<14}│")
        print(f"  └──────────────────────────────────────────────────┘")
    print()
    streamer = upstox_client.MarketDataStreamerV3(client, instrument_keys, mode)
    tick_count = [0]
    def on_open():
        print("  ✓ WebSocket connected. Receiving data...\n")
    def on_message(message):
        now = datetime.now(IST)
        ts = now.isoformat()
        time_str = now.strftime("%H:%M:%S.%f")[:-3]
        if hasattr(message, "to_dict"):
            data = message.to_dict()
        elif isinstance(message, dict):
            data = message
        elif isinstance(message, str):
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                data = {"raw": message}
        else:
            data = {"raw": str(message)}
        msg_type = data.get("type", "")
        feeds = data.get("feeds", {})
        if msg_type == "market_info":
            seg = data.get("marketInfo") or data.get("market_info") or {}
            statuses = seg.get("segmentStatus") or seg.get("segment_status") or {}
            print(f"  [market_info] {list(statuses.keys())}")
            writer.write("_market_info", data, ts)
            return
        if feeds:
            for key, feed_data in feeds.items():
                writer.write(key, feed_data, ts)
                tick_count[0] += 1
                c = key_colors.get(key, "")
                name = key_names.get(key, key)
                ltp = extract_ltp(feed_data)
                close = extract_close(feed_data)
                bid, ask = extract_best_bid_ask(feed_data)
                oi = extract_field(feed_data, "oi")
                vol = extract_field(feed_data, "v") or extract_field(feed_data, "vtt")
                atp = extract_field(feed_data, "atp")
                iv = extract_greek(feed_data, "iv")
                delta = extract_greek(feed_data, "delta")
                theta = extract_greek(feed_data, "theta")
                gamma = extract_greek(feed_data, "gamma")
                vega = extract_greek(feed_data, "vega")
                parts = []
                if ltp is not None:
                    ltp_str = f"₹{ltp}"
                    if close and close != 0:
                        chg = ltp - close
                        pct = (chg / close) * 100
                        sign = "+" if chg >= 0 else ""
                        ltp_str += f" ({sign}{pct:.1f}%)"
                    parts.append(ltp_str)
                if bid is not None and ask is not None:
                    parts.append(f"B:{bid} A:{ask}")
                if iv is not None:
                    parts.append(f"IV:{iv:.1f}")
                if delta is not None:
                    parts.append(f"Δ:{delta:.3f}")
                if theta is not None:
                    parts.append(f"θ:{theta:.1f}")
                if gamma is not None:
                    parts.append(f"γ:{gamma:.4f}")
                if vega is not None:
                    parts.append(f"ν:{vega:.1f}")
                if oi is not None:
                    parts.append(f"OI:{int(oi):,}")
                if vol is not None:
                    parts.append(f"V:{int(vol):,}")
                if atp is not None:
                    parts.append(f"ATP:{atp}")
                info = " │ ".join(parts) if parts else "(waiting)"
                print(f"  {time_str} #{tick_count[0]:<6} {c}{name:<22}{RESET} {info}")
                if strategy_runner and ltp is not None:
                    strategy_runner.on_tick(key, ltp, ts, feed_data)
        else:
            writer.write("_other", data, ts)
    def on_error(error):
        print(f"  ✗ Error: {error}")
    def on_close(*args):
        print("  WebSocket closed.")
    streamer.on("open", on_open)
    streamer.on("message", on_message)
    streamer.on("error", on_error)
    streamer.on("close", on_close)
    streamer.auto_reconnect(True, 5, 50)
    try:
        streamer.connect()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            streamer.disconnect()
        except Exception:
            pass
        writer.close()
        print(f"\n  ── Stopped ──")
        print(f"  Total ticks: {tick_count[0]:,}")
        for k, count in sorted(writer.stats.items()):
            name = key_names.get(k, k.split("|")[-1] if "|" in k else k)
            c = key_colors.get(k, "")
            print(f"    {c}{name:>30}{RESET}: {count:,} ticks")
        print(f"  Files in: {writer.dir}/")
        if strategy_runner:
            strategy_runner.print_final_summary()
def extract_ltp(feed_data):
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

def extract_field(feed_data, field):
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
def extract_greek(feed_data, greek_name):
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
def extract_best_bid_ask(feed_data):
    if not isinstance(feed_data, dict):
        return None, None
    for ff_key in ("ff", "fullFeed", "full_feed"):
        ff = feed_data.get(ff_key) or {}
        for mff_key in ("marketFF", "indexFF", "market_ff", "index_ff"):
            mff = ff.get(mff_key) or {}
            ml = mff.get("marketLevel") or mff.get("market_level") or {}
            baq = ml.get("bidAskQuote") or ml.get("bid_ask_quote") or []
            if baq:
                lvl = baq[0]
                bid = lvl.get("bidP") or lvl.get("bid_p") or lvl.get("bp")
                ask = lvl.get("askP") or lvl.get("ask_p") or lvl.get("ap")
                return bid, ask
    for flwg_key in ("firstLevelWithGreeks", "first_level_with_greeks"):
        flwg = feed_data.get(flwg_key) or {}
        fd = flwg.get("firstDepth") or flwg.get("first_depth") or {}
        if fd:
            bid = fd.get("bidP") or fd.get("bid_p")
            ask = fd.get("askP") or fd.get("ask_p")
            return bid, ask
    return None, None

def extract_ohlc(feed_data):
    """Extract daily OHLC (open, high, low, close) from feed data.
    Handles the actual protobuf structure: ff.marketFF.marketOHLC.ohlc[]
    where ohlc is an ARRAY of intervals (1d, I1, I30). We want "1d".
    Also handles simpler structures where ohlc is a direct dict.
    Returns dict with keys: open, high, low, close. Values may be None."""
    result = {"open": None, "high": None, "low": None, "close": None}
    if not isinstance(feed_data, dict):
        return result
    for ff_key in ("ff", "fullFeed", "full_feed"):
        ff = feed_data.get(ff_key) or {}
        for mff_key in ("marketFF", "indexFF", "market_ff", "index_ff"):
            mff = ff.get(mff_key) or {}
            # Path 1: marketOHLC.ohlc[] — protobuf streamer (full_d30/full)
            market_ohlc = mff.get("marketOHLC") or mff.get("market_ohlc") or {}
            ohlc_list = market_ohlc.get("ohlc") or []
            if isinstance(ohlc_list, list):
                for entry in ohlc_list:
                    if not isinstance(entry, dict):
                        continue
                    interval = entry.get("interval") or entry.get("intv") or ""
                    # "1d" = daily, prefer this; also accept first entry as fallback
                    if interval == "1d" or interval == "1D":
                        for k in ("open", "high", "low", "close"):
                            if entry.get(k) is not None:
                                result[k] = entry[k]
                        return result
                # No "1d" found — use first entry as fallback
                if ohlc_list and isinstance(ohlc_list[0], dict):
                    for k in ("open", "high", "low", "close"):
                        if ohlc_list[0].get(k) is not None:
                            result[k] = ohlc_list[0][k]
                    if result["low"] is not None:
                        return result
            # Path 2: ohlc as direct dict (some REST/older formats)
            ohlc = mff.get("ohlc") or {}
            if isinstance(ohlc, dict) and ohlc:
                for k in ("open", "high", "low", "close"):
                    if ohlc.get(k) is not None:
                        result[k] = ohlc[k]
                if result["low"] is not None:
                    return result
    return result

# ═══════════════════════════════════════════════════════════════
# STRATEGY FRAMEWORK — Paper Trading with PnL
# ═══════════════════════════════════════════════════════════════
class PaperTrader:
    def __init__(self, instrument_name, lot_size=1, max_trades=50):
        self.instrument_name = instrument_name
        self.lot_size = lot_size
        self.max_trades = max_trades
        self.in_position = False
        self.entry_price = None
        self.entry_time = None
        self.peak_since_entry = None
        self.trades = []
        self.total_pnl = 0.0
        self.current_ltp = None
        self.current_time = None
    def can_buy(self):
        return not self.in_position and len(self.trades) < self.max_trades
    def can_sell(self):
        return self.in_position
    def buy(self, price, timestamp):
        if not self.can_buy():
            return False
        self.in_position = True
        self.entry_price = price
        self.entry_time = timestamp
        self.peak_since_entry = price
        self._print_action("BUY", price, timestamp)
        return True
    def sell(self, price, timestamp):
        if not self.can_sell():
            return False
        pnl = (price - self.entry_price) * self.lot_size
        pnl_pct = ((price - self.entry_price) / self.entry_price) * 100 if self.entry_price else 0
        trade = {
            "trade_no": len(self.trades) + 1,
            "entry_time": self.entry_time, "entry_price": self.entry_price,
            "exit_time": timestamp, "exit_price": price,
            "pnl": pnl, "pnl_pct": pnl_pct, "peak": self.peak_since_entry,
        }
        self.trades.append(trade)
        self.total_pnl += pnl
        self._print_action("SELL", price, timestamp)
        self.print_trade_table()
        self.in_position = False
        self.entry_price = None
        self.entry_time = None
        self.peak_since_entry = None
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
        pnl_pct = ((self.current_ltp - self.entry_price) / self.entry_price) * 100 if self.entry_price else 0
        return pnl, pnl_pct
    def status_line(self, strategy=None):
        if self.in_position:
            ur_pnl, ur_pct = self.unrealized_pnl()
            sign = "+" if ur_pnl >= 0 else ""
            stop_info = ""
            if strategy and hasattr(strategy, 'stop_loss') and strategy.stop_loss is not None:
                bev_tag = " BEV✓" if getattr(strategy, '_breakeven_active', False) else ""
                stop_info = f" │ SL: ₹{strategy.stop_loss:.2f}{bev_tag} │ Peak: ₹{strategy.peak_price:.2f}"
            return (
                f"\033[1m[STRATEGY]\033[0m LONG @ ₹{self.entry_price:.2f} │ "
                f"LTP: ₹{self.current_ltp:.2f} │ "
                f"Unrealized: {sign}₹{ur_pnl:.2f} ({sign}{ur_pct:.1f}%){stop_info} │ "
                f"Realized: ₹{self.total_pnl:.2f} │ "
                f"Trades: {len(self.trades)}/{self.max_trades}"
            )
        else:
            return (
                f"\033[1m[STRATEGY]\033[0m NO POSITION │ "
                f"Realized: ₹{self.total_pnl:.2f} │ "
                f"Trades: {len(self.trades)}/{self.max_trades}"
            )
    def _print_action(self, action, price, timestamp):
        if isinstance(timestamp, str):
            time_str = timestamp.split("T")[-1][:12] if "T" in timestamp else timestamp
        else:
            time_str = timestamp.strftime("%H:%M:%S.%f")[:-3]
        color = "\033[92m" if action == "BUY" else "\033[91m"
        reset = "\033[0m"
        print(f"\n  {color}{'═'*60}")
        print(f"  ▶ {action} @ ₹{price:.2f}  │  {time_str}  │  {self.instrument_name}")
        if action == "SELL" and self.trades:
            t = self.trades[-1]
            sign = "+" if t["pnl"] >= 0 else ""
            print(f"    P&L: {sign}₹{t['pnl']:.2f} ({sign}{t['pnl_pct']:.1f}%)")
        print(f"  {'═'*60}{reset}\n")
    def print_trade_table(self):
        if not self.trades:
            return
        print(f"\n  {'─'*90}")
        print(f"  {'#':>4} │ {'Entry Time':>12} │ {'Entry ₹':>10} │ {'Exit Time':>12} │ {'Exit ₹':>10} │ {'P&L ₹':>10} │ {'P&L %':>8}")
        print(f"  {'─'*4} │ {'─'*12} │ {'─'*10} │ {'─'*12} │ {'─'*10} │ {'─'*10} │ {'─'*8}")
        for t in self.trades:
            entry_t = t["entry_time"].split("T")[-1][:8] if isinstance(t["entry_time"], str) and "T" in t["entry_time"] else str(t["entry_time"])[-8:]
            exit_t = t["exit_time"].split("T")[-1][:8] if isinstance(t["exit_time"], str) and "T" in t["exit_time"] else str(t["exit_time"])[-8:]
            sign = "+" if t["pnl"] >= 0 else ""
            color = "\033[92m" if t["pnl"] >= 0 else "\033[91m"
            reset = "\033[0m"
            print(f"  {t['trade_no']:>4} │ {entry_t:>12} │ {t['entry_price']:>10.2f} │ {exit_t:>12} │ {t['exit_price']:>10.2f} │ {color}{sign}{t['pnl']:>9.2f}{reset} │ {color}{sign}{t['pnl_pct']:>7.1f}%{reset}")
        print(f"  {'─'*4} │ {'─'*12} │ {'─'*10} │ {'─'*12} │ {'─'*10} │ {'─'*10} │ {'─'*8}")
        total_sign = "+" if self.total_pnl >= 0 else ""
        total_color = "\033[92m" if self.total_pnl >= 0 else "\033[91m"
        reset = "\033[0m"
        winners = sum(1 for t in self.trades if t["pnl"] > 0)
        print(f"  {'SUM':>4} │ {'':>12} │ {'':>10} │ {'':>12} │ {'':>10} │ {total_color}{total_sign}{self.total_pnl:>9.2f}{reset} │ W:{winners}/{len(self.trades)}")
        print(f"  {'─'*90}\n")
class Strategy:
    name = "Base"
    description = "Override this"
    def __init__(self):
        self.tick_count = 0
    def on_tick(self, ltp, timestamp, feed_data=None):
        raise NotImplementedError
    def configure(self):
        pass
    def describe(self):
        print(f"  Strategy: {self.name}")
        print(f"  {self.description}")
class MomentumTrailingStop(Strategy):
    name = "Buy Rise + Trailing Stop"
    description = "Buy when price rises, trailing stop locks profits as it climbs"
    def __init__(self, entry_rise=5.0, trail_points=3.0, lookback=100):
        super().__init__()
        self.entry_rise = entry_rise
        self.trail_points = trail_points
        self.lookback = lookback
        self.prices = []
        self.peak_price = None
        self.stop_loss = None
        self._in_position = False
    def on_tick(self, ltp, timestamp, feed_data=None):
        self.tick_count += 1
        self.prices.append(ltp)
        if len(self.prices) > self.lookback + 100:
            self.prices = self.prices[-(self.lookback + 100):]
        if self._in_position:
            if ltp > self.peak_price:
                self.peak_price = ltp
                self.stop_loss = self.peak_price - self.trail_points
            if ltp <= self.stop_loss:
                self._in_position = False
                self.peak_price = None
                self.stop_loss = None
                return "SELL"
            return None
        if len(self.prices) < 10:
            return None
        window = self.prices[-self.lookback:] if len(self.prices) >= self.lookback else self.prices
        recent_low = min(window)
        if ltp >= recent_low + self.entry_rise:
            self._in_position = True
            self.peak_price = ltp
            self.stop_loss = ltp - self.trail_points
            return "BUY"
        return None
    def configure(self):
        print(f"\n  Buy Rise + Trailing Stop Parameters:")
        print(f"    entry_rise:   Price must rise this many points from recent low to BUY")
        print(f"    trail_points: Stop loss trails this far below the peak")
        print(f"    lookback:     How many ticks to look back for the recent low")
        val = input(f"\n  Entry rise points [{self.entry_rise}]: ").strip()
        if val:

            try:

                self.entry_rise = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        val = input(f"  Trail stop points [{self.trail_points}]: ").strip()
        if val:

            try:

                self.trail_points = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        val = input(f"  Lookback ticks [{self.lookback}]: ").strip()
        if val:

            try:

                self.lookback = int(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        print(f"  ✓ Set: entry_rise={self.entry_rise}, trail={self.trail_points}, lookback={self.lookback}")
    def describe(self):
        super().describe()
        print(f"  Params: entry_rise={self.entry_rise}, trail_points={self.trail_points}, lookback={self.lookback} ticks")
        if self._in_position and self.stop_loss:
            print(f"  Live: peak=₹{self.peak_price:.2f}, stop_loss=₹{self.stop_loss:.2f}")
STRATEGIES = {
    "1": {"name": "Buy Rise + Trailing Stop", "class": MomentumTrailingStop},
    "2": {"name": "Option + Future Momentum (CE only)", "class": None},
    "3": {"name": "Dual CE+PE Momentum", "class": None},
    "4": {"name": "Option + Future Momentum (PE only)", "class": None},
}
class OptionFutureMomentum(Strategy):
    """Strategy 2: Option + Future Momentum with Tiered Trailing Stop + Breakeven Lock"""
    description = "Buy when option+future both confirm momentum, tiered trailing stop + breakeven lock"
    def __init__(self, side="CALL"):
        super().__init__()
        self.side = side.upper()
        self.name = f"Option+Fut Momentum ({self.side})"
        self.future_key = None
        self.opt_low_margin = 5
        self.fut_low_margin = 10
        self.trail_mode = "pct"
        self.trail_tiers = [(0, 1), (2, 1), (6, 2), (15, 5)]
        self.breakeven_trigger_pct = 2.0   # 0 = activate BEV on ANY price increase
        self.breakeven_buffer = 1.0
        self.bev_buffer_mode = "rupees"    # "rupees" or "pct" (default: 1% of entry)
        self.straight_down_stop_pct = 25.0   # wider stop when price never went above entry
        # Threshold to switch from wide stop to normal trail = BEV trigger
        # This closes the gap: either wide stop protects you OR BEV protects you
        self.fut_move_limit = -1          # max future move from prev close before blocking entry (0=disabled)
        self.fut_prev_close = None          # previous day close of future (from cp field)
        # ── Entry filters: avoid buying at the high ──
        self.opt_day_open = None            # option's opening price today
        self.opt_prev_day_close = None      # option's previous day close
        self.opt_atp = None                 # option's average traded price today
        self.opt_delta = None               # option delta (from exchange)
        self.opt_theta = None               # option theta
        self.opt_gamma = None               # option gamma
        self.opt_vega = None                # option vega
        self.opt_iv = None                  # option implied volatility
        self.opt_tbq = None                 # total buy quantity (pending bids)
        self.opt_tsq = None                 # total sell quantity (pending asks)
        self.opt_open_rally_pct = 50        # max allowed rally (%) before blocking
        self.opt_atp_distance_pct = 30      # max allowed distance above ATP (%) before blocking
        self.opt_buy_sell_ratio = 0.0       # min tbq/tsq ratio (0 = disabled)
        self.rally_base = "daylow_prevclose"  # "open" or "daylow_prevclose"
        self._breakeven_active = False
        self._breakeven_stop = None
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
        self._buy_check_pending = False   # only True on first tick after interval rolls
        self._minutes_seen = 0            # need at least 1 full minute before trading
        self._interval_counter = 0        # counts minute boundaries within current interval
        self.minute_interval = 1          # check every N minutes (configurable)
        self.opt_momentum_threshold = 1.0  # LTP must exceed prev_avg by this Rs
        # ── OHLC entry filters (from backtest analysis) ──
        self.require_lower_wick = False    # block buy if prev candle has no lower wick
        self.block_marubozu = 0            # block if body/range > this (0=off, 0.80=block 80%+)
        self.max_range = 0                 # block if prev H-L > this Rs (0=off)
        self.max_momentum = 0              # block if LTP > avg + this Rs (0=off)
        self.max_above_high = 0            # block if LTP > prev High + this Rs (0=off)
        # ── Trade management ──
        self.rebuy_drop = 0               # after sell, require LTP drop this % before re-buy (0=off)
        self.max_hold = 0                 # force exit after N minutes (0=off)
        self.target_profit = 0            # sell at +N% instead of trail (0=off)
        self.first_only = False           # one trade per side per day
        self._last_entry_price = None     # for rebuy_drop tracking
        self._first_traded = False        # for first_only tracking
        self._entry_timestamp = None      # for max_hold tracking
    def _get_minute_key(self, timestamp):
        if isinstance(timestamp, str):
            return timestamp[:16]
        return timestamp.strftime("%Y-%m-%dT%H:%M")
    def _roll_minute(self, minute_key):
        if self._current_minute is None:
            self._current_minute = minute_key
            return
        if minute_key != self._current_minute:
            # Daily reset: if the date changed or it's 9:15, reset daily state
            old_date = self._current_minute[:10]
            new_date = minute_key[:10]
            new_time = minute_key[11:16] if len(minute_key) > 15 else ""
            if old_date != new_date or new_time == "09:15":
                self._first_traded = False
                self._last_entry_price = None
            self._current_minute = minute_key
            self._interval_counter += 1

            if self._interval_counter < self.minute_interval:
                # Still accumulating ticks within the N-minute interval
                return

            # N minutes reached — compute averages from all accumulated ticks
            parts = []
            if self._min_opt_ltps:
                self.opt_prev_avg_ltp = sum(self._min_opt_ltps) / len(self._min_opt_ltps)
                self.opt_prev_min_open = self._min_opt_ltps[0]
                self.opt_prev_min_high = max(self._min_opt_ltps)
                self.opt_prev_min_low = min(self._min_opt_ltps)
                self.opt_prev_min_close = self._min_opt_ltps[-1]
                parts.append(f"OptLTP={self.opt_prev_avg_ltp:.2f}({len(self._min_opt_ltps)}t)")
            if self._min_opt_vols:
                self.opt_prev_avg_vol = sum(self._min_opt_vols) / len(self._min_opt_vols)
                parts.append(f"OptVol={self.opt_prev_avg_vol:,.0f}")
            if self._min_fut_ltps:
                self.fut_prev_avg_ltp = sum(self._min_fut_ltps) / len(self._min_fut_ltps)
                parts.append(f"FutLTP={self.fut_prev_avg_ltp:.2f}({len(self._min_fut_ltps)}t)")
            if parts:
                minute_label = self._current_minute[-5:]
                side_tag = f" {self.side}" if hasattr(self, 'side') else ""
                avgs = " │ ".join(parts)
                print(f"  \033[90m[1m-AVG{side_tag} {minute_label}] {avgs}\033[0m")

                # ── Per-minute strategy condition check ──
                if not self._in_position and self.opt_prev_avg_ltp and self.fut_prev_avg_ltp:
                    checks = []
                    opt_ok = self.opt_ltp is not None and self.opt_ltp > self.opt_prev_avg_ltp
                    checks.append(f"opt>avg:{'✓' if opt_ok else '✗'}({self.opt_ltp:.1f} vs {self.opt_prev_avg_ltp:.1f})" if self.opt_ltp else "opt>avg:? (no ltp)")

                    daylow_ok = self.opt_ltp is not None and self.opt_day_low is not None and self.opt_ltp > self.opt_day_low + self.opt_low_margin
                    checks.append(f"opt>low+m:{'✓' if daylow_ok else '✗'}({self.opt_ltp:.1f} vs {(self.opt_day_low or 0)+self.opt_low_margin:.1f})" if self.opt_ltp else "opt>low:?")

                    vol_ok = self.opt_vol is not None and self.opt_prev_avg_vol is not None and self.opt_vol > self.opt_prev_avg_vol
                    checks.append(f"vol:{'✓' if vol_ok else '✗'}")

                    if self.fut_ltp is not None:
                        if self.side == "CALL":
                            fut_ok = self.fut_ltp > self.fut_prev_avg_ltp
                            fut_daylow_ok = self.fut_day_low is not None and self.fut_ltp > self.fut_day_low + self.fut_low_margin
                            checks.append(f"fut↑avg:{'✓' if fut_ok else '✗'}({self.fut_ltp:.1f} vs {self.fut_prev_avg_ltp:.1f})")
                            checks.append(f"fut>low+m:{'✓' if fut_daylow_ok else '✗'}")
                        else:
                            fut_ok = self.fut_ltp < self.fut_prev_avg_ltp
                            fut_dayhigh_ok = self.fut_day_high is not None and self.fut_ltp < self.fut_day_high - self.fut_low_margin
                            checks.append(f"fut↓avg:{'✓' if fut_ok else '✗'}({self.fut_ltp:.1f} vs {self.fut_prev_avg_ltp:.1f})")
                            checks.append(f"fut<high-m:{'✓' if fut_dayhigh_ok else '✗'}")
                    else:
                        checks.append("fut:? (no tick)")

                    all_pass = opt_ok and daylow_ok and vol_ok and self.fut_ltp is not None and fut_ok
                    status = "\033[92mREADY" if all_pass else "\033[90mWAITING"
                    print(f"  \033[90m[{self.side} CHECK] {' │ '.join(checks)} → {status}\033[0m")
                elif self._in_position:
                    gain_pct = ((self.opt_ltp - self.entry_price) / self.entry_price) * 100 if self.opt_ltp and self.entry_price else 0
                    bev = " BEV✓" if self._breakeven_active else ""
                    print(f"  \033[90m[{self.side} CHECK] IN POSITION │ entry ₹{self.entry_price:.2f} │ now ₹{self.opt_ltp:.2f} │ {'+' if gain_pct>=0 else ''}{gain_pct:.1f}% │ stop ₹{self.stop_loss:.2f}{bev}\033[0m")
            self._min_opt_ltps = []
            self._min_opt_vols = []
            self._min_fut_ltps = []
            self._interval_counter = 0
            self._minutes_seen += 1
            # Gate BUY: only allow check on first tick of new minute,
            # and only after at least 1 full minute of data collected
            if self._minutes_seen >= 1 and self.opt_prev_avg_ltp is not None and self.fut_prev_avg_ltp is not None:
                self._buy_check_pending = True
    def _get_trail_distance(self, gain_or_pct):
        trail_val = self.trail_tiers[0][1]
        for threshold, tv in self.trail_tiers:
            if gain_or_pct >= threshold:
                trail_val = tv
        if self.trail_mode == "pct":
            return self.entry_price * (trail_val / 100)
        else:
            return trail_val
    def on_option_tick(self, ltp, timestamp, feed_data=None):
        self.tick_count += 1
        ltp = float(ltp)
        self.opt_ltp = ltp
        vol = extract_field(feed_data, "v") or extract_field(feed_data, "vtt")
        if vol is not None:
            try:
                self.opt_vol = float(vol)
            except (ValueError, TypeError):
                pass
        if self.opt_day_low is None or ltp < self.opt_day_low:
            self.opt_day_low = ltp
        # Use real market OHLC day low if available (more accurate than script-tracked)
        if feed_data:
            ohlc = extract_ohlc(feed_data)
            if ohlc["low"] is not None and ohlc["low"] > 0:
                self.opt_day_low = ohlc["low"]
            if self.opt_day_open is None and ohlc["open"] is not None and ohlc["open"] > 0:
                self.opt_day_open = ohlc["open"]
            # Capture option's previous day close
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
            # Extract greeks from feed
            _gd = extract_greek(feed_data, "delta")
            if _gd is not None: self.opt_delta = _gd
            _gt = extract_greek(feed_data, "theta")
            if _gt is not None: self.opt_theta = _gt
            _gg = extract_greek(feed_data, "gamma")
            if _gg is not None: self.opt_gamma = _gg
            _gv = extract_greek(feed_data, "vega")
            if _gv is not None: self.opt_vega = _gv
            _gi = extract_greek(feed_data, "iv")
            if _gi is not None: self.opt_iv = _gi
        minute_key = self._get_minute_key(timestamp)
        self._roll_minute(minute_key)
        self._min_opt_ltps.append(ltp)
        if self.opt_vol is not None:
            self._min_opt_vols.append(float(self.opt_vol))
        # ── Rebuy-drop: checked at buy time, not tracked per-tick ──
        if self._in_position:
            if ltp > self.peak_price:
                self.peak_price = ltp
            # ── Target profit exit ──
            if self.target_profit > 0 and self.entry_price:
                target_price = self.entry_price * (1 + self.target_profit / 100)
                if ltp >= target_price:
                    entry_was = self.entry_price
                    self._in_position = False
                    self.entry_price = None
                    self.peak_price = None
                    self.stop_loss = None
                    self._breakeven_active = False
                    self._breakeven_stop = None
                    self._sell_minute = self._current_minute
                    self._interval_counter = 0
                    print(f"  \033[92m[{self.side}] ✓ TARGET HIT +{self.target_profit}%: entry={entry_was:.0f} exit={ltp:.0f} P&L=+{ltp-entry_was:.0f}\033[0m")
                    return "SELL"
            # ── Max hold time exit (only if BEV never activated AND no target_profit) ──
            if self.max_hold > 0 and not self._breakeven_active and self.target_profit <= 0 and self._entry_timestamp:
                try:
                    et = self._entry_timestamp
                    eh, em = int(et[11:13]), int(et[14:16])
                    ts_str = timestamp if isinstance(timestamp, str) else str(timestamp)
                    th, tm = int(ts_str[11:13]), int(ts_str[14:16])
                    held = (th * 60 + tm) - (eh * 60 + em)
                    if held >= self.max_hold:
                        self._in_position = False
                        self.entry_price = None
                        self.peak_price = None
                        self.stop_loss = None
                        self._breakeven_active = False
                        self._breakeven_stop = None
                        self._sell_minute = self._current_minute
                        self._interval_counter = 0
                        print(f"  \033[93m[{self.side}] MAX HOLD {self.max_hold}min reached → SELL\033[0m")
                        return "SELL"
                except:
                    pass
            # ── Two-tier stop loss ──
            # If price has NEVER gone above entry → wider stop (straight-down protection)
            # Two-tier: wide stop until BEV can activate, then normal trail + BEV
            bev_threshold = self.entry_price * (self.breakeven_trigger_pct / 100)
            price_went_up = self.peak_price >= self.entry_price + bev_threshold
            if not price_went_up:
                # STRAIGHT DOWN: price never went above entry — use wider stop
                wide_stop = self.entry_price * (1 - self.straight_down_stop_pct / 100)
                self.stop_loss = wide_stop
            else:
                # NORMAL: price went above entry — use tiered trail + BEV
                if self.trail_mode == "pct":
                    gain = ((self.peak_price - self.entry_price) / self.entry_price) * 100
                else:
                    gain = self.peak_price - self.entry_price
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
                        print(f"  \033[96m[{self.side}] BREAKEVEN LOCKED │ peak ₹{self.peak_price:.2f} (+{peak_gain_pct:.1f}%) │ floor: ₹{self._breakeven_stop:.2f}\033[0m")
                if self._breakeven_active and self._breakeven_stop is not None:
                    self.stop_loss = max(trail_stop, self._breakeven_stop)
                else:
                    self.stop_loss = trail_stop
            if ltp <= self.stop_loss:
                # If target_profit mode: ignore normal stops, keep holding
                if self.target_profit > 0:
                    # Re-sync: keep position alive, reset stop to wide
                    self.stop_loss = self.entry_price * (1 - self.straight_down_stop_pct / 100)
                    return None
                pnl = ltp - self.entry_price
                pnl_pct = (pnl / self.entry_price) * 100
                if not price_went_up:
                    exit_tag = " [WIDE]"
                elif self._breakeven_active and self._breakeven_stop is not None and self.stop_loss <= self._breakeven_stop + 0.01:
                    exit_tag = " [BEV]"
                elif self._breakeven_active:
                    exit_tag = " [TRAIL]"
                else:
                    exit_tag = ""
                print(f"  \033[91m[{self.side} SIGNAL] ── STOP HIT → SELL ──\033[0m")
                print(f"  \033[91m  Entry: ₹{self.entry_price:.2f} │ Peak: ₹{self.peak_price:.2f} │ Exit: ₹{ltp:.2f}{exit_tag}\033[0m")
                print(f"  \033[91m  P&L: {'+' if pnl>=0 else ''}₹{pnl:.2f} ({'+' if pnl_pct>=0 else ''}{pnl_pct:.1f}%) │ Stop was: ₹{self.stop_loss:.2f}\033[0m")
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
        # ── Not in position: check entry conditions ──
        # Only check on the FIRST option tick after a minute rolls
        if not self._buy_check_pending:
            return None
        self._buy_check_pending = False   # consume the flag — one check per minute
        if self._sell_minute is not None:
            if self._current_minute == self._sell_minute:
                return None
            self._sell_minute = None
        if self.opt_prev_avg_ltp is None or self.fut_prev_avg_ltp is None:
            return None
        if self.opt_prev_avg_vol is None:
            return None
        if ltp <= self.opt_prev_avg_ltp + self.opt_momentum_threshold:
            return None
        if ltp <= (self.opt_day_low or 0) + self.opt_low_margin:
            return None
        if self.opt_vol is not None and self.opt_vol <= self.opt_prev_avg_vol:
            return None
        if self.fut_ltp is None:
            return None
        if self.side == "CALL":
            if self.fut_ltp <= self.fut_prev_avg_ltp:
                return None
            if self.fut_ltp <= (self.fut_day_low or 0) + self.fut_low_margin:
                return None
        else:
            if self.fut_ltp >= self.fut_prev_avg_ltp:
                return None
            if self.fut_day_high is None:
                return None
            if self.fut_ltp >= self.fut_day_high - self.fut_low_margin:
                return None
        # Future-from-close filter: don't buy options that already had their big move
        filter_results = []
        # Auto-detect fut_move_limit if -1 (same as backtest)
        if self.fut_move_limit < 0 and self.future_key:
            fk = self.future_key.upper()
            if "BANKNIFTY" in fk or "BANKEX" in fk:
                self.fut_move_limit = 200
            else:
                self.fut_move_limit = 75
        if self.fut_prev_close is not None and self.fut_move_limit > 0:
            fut_move = self.fut_ltp - self.fut_prev_close
            if self.side == "CALL" and fut_move > self.fut_move_limit:
                filter_results.append(f"futMove:✗({fut_move:+.0f}>{self.fut_move_limit:.0f})")
                print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[91mBLOCKED\033[0m")
                return None   # market already up too much, calls are pumped
            if self.side == "PUT" and fut_move < -self.fut_move_limit:
                filter_results.append(f"futMove:✗({fut_move:+.0f}<-{self.fut_move_limit:.0f})")
                print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[91mBLOCKED\033[0m")
                return None   # market already down too much, puts are pumped
            filter_results.append(f"futMove:✓({fut_move:+.0f})")
        else:
            filter_results.append("futMove:off")
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
                if rally > self.opt_open_rally_pct:
                    filter_results.append(f"rally:✗({rally:.0f}%>{self.opt_open_rally_pct:.0f}%)")
                    print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[91mBLOCKED\033[0m")
                    return None   # option already up 50%+ from open
                filter_results.append(f"rally:✓({rally:.0f}%)")
            else:
                filter_results.append("rally:nobase")
        else:
            filter_results.append("rally:off")
        # ATP filter: don't buy if option is too far above day's average traded price
        if self.opt_atp is not None and self.opt_atp > 0 and self.opt_atp_distance_pct > 0:
            atp_dist = (ltp - self.opt_atp) / self.opt_atp * 100
            if atp_dist > self.opt_atp_distance_pct:
                filter_results.append(f"atp:✗({atp_dist:.0f}%>{self.opt_atp_distance_pct:.0f}%)")
                print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[91mBLOCKED\033[0m")
                return None   # buying way above market consensus price
            filter_results.append(f"atp:✓({atp_dist:.0f}%)")
        else:
            filter_results.append("atp:off")
        # Buy/Sell queue ratio: don't buy if sellers dominate
        if self.opt_buy_sell_ratio > 0 and self.opt_tsq is not None and self.opt_tsq > 0:
            ratio = (self.opt_tbq or 0) / self.opt_tsq
            if ratio < self.opt_buy_sell_ratio:
                filter_results.append(f"bs:✗({ratio:.1f}<{self.opt_buy_sell_ratio:.1f})")
                print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[91mBLOCKED\033[0m")
                return None   # sellers outnumber buyers, momentum might reverse
            filter_results.append(f"bs:✓({ratio:.1f})")
        else:
            filter_results.append("bs:off")
        
        # ── OHLC candle filters ──
        ohlc_results = []
        O = self.opt_prev_min_open
        H = self.opt_prev_min_high
        L = self.opt_prev_min_low
        C = self.opt_prev_min_close
        A = self.opt_prev_avg_ltp
        if O is not None and H is not None and L is not None and C is not None:
            rng = H - L
            lower_wick = min(O, C) - L
            body_ratio = abs(C - O) / rng if rng > 0 else 0
            if self.require_lower_wick:
                if lower_wick < 0.01:
                    ohlc_results.append(f"wick:✗(no lower wick)")
                    print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[92mPASSED\033[0m")
                    print(f"  [{self.side} OHLC] {' │ '.join(ohlc_results)} → \033[91mBLOCKED\033[0m")
                    return None
                ohlc_results.append("wick:✓")
            if self.block_marubozu > 0:
                if body_ratio >= self.block_marubozu:
                    ohlc_results.append(f"maru:✗({body_ratio:.0%}>{self.block_marubozu:.0%})")
                    print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[92mPASSED\033[0m")
                    print(f"  [{self.side} OHLC] {' │ '.join(ohlc_results)} → \033[91mBLOCKED\033[0m")
                    return None
                ohlc_results.append(f"maru:✓({body_ratio:.0%})")
            if self.max_range > 0:
                if rng > self.max_range:
                    ohlc_results.append(f"rng:✗({rng:.1f}>{self.max_range})")
                    print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[92mPASSED\033[0m")
                    print(f"  [{self.side} OHLC] {' │ '.join(ohlc_results)} → \033[91mBLOCKED\033[0m")
                    return None
                ohlc_results.append(f"rng:✓({rng:.1f})")
        if self.max_momentum > 0 and A is not None and ltp > A + self.max_momentum:
            ohlc_results.append(f"mom:✗({ltp-A:.1f}>{self.max_momentum})")
            print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[92mPASSED\033[0m")
            print(f"  [{self.side} OHLC] {' │ '.join(ohlc_results)} → \033[91mBLOCKED\033[0m")
            return None
        if self.max_above_high > 0 and H is not None and ltp > H + self.max_above_high:
            ohlc_results.append(f"abvH:✗({ltp-H:.1f}>{self.max_above_high})")
            print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[92mPASSED\033[0m")
            print(f"  [{self.side} OHLC] {' │ '.join(ohlc_results)} → \033[91mBLOCKED\033[0m")
            return None
        # ── Trade management: first_only ──
        if self.first_only and self._first_traded:
            print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[92mPASSED\033[0m")
            print(f"  [{self.side}] First-only: already traded today → \033[91mBLOCKED\033[0m")
            return None
        # ── Trade management: rebuy_drop ──
        if self.rebuy_drop > 0 and self._last_entry_price is not None:
            drop_target = self._last_entry_price * (1 - self.rebuy_drop / 100)
            if ltp > drop_target:
                print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[92mPASSED\033[0m")
                print(f"  [{self.side}] Rebuy-drop: LTP {ltp:.0f} > {drop_target:.0f} ({self.rebuy_drop}% below {self._last_entry_price:.0f}) → \033[91mBLOCKED\033[0m")
                return None
        
        if ohlc_results:
            print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[92mPASSED\033[0m")
            print(f"  [{self.side} OHLC] {' │ '.join(ohlc_results)} → \033[92mPASSED\033[0m")
        else:
            print(f"  [{self.side} FILTERS] {' │ '.join(filter_results)} → \033[92mPASSED\033[0m")
        
        self._in_position = True
        self.entry_price = ltp
        self.peak_price = ltp
        self._breakeven_active = False
        self._breakeven_stop = None
        trail = self._get_trail_distance(0)
        self.stop_loss = ltp - trail
        self._entry_timestamp = timestamp
        self._last_entry_price = ltp
        self._first_traded = True

        # ── Signal debug: show WHY we're buying ──
        fut_dir = "↑" if self.side == "CALL" else "↓"
        fut_delta = self.fut_ltp - self.fut_prev_avg_ltp if self.fut_prev_avg_ltp else 0
        opt_delta = ltp - self.opt_prev_avg_ltp if self.opt_prev_avg_ltp else 0
        print(f"  \033[93m[{self.side} SIGNAL] ── BUY CONDITIONS MET ──\033[0m")
        print(f"  \033[93m  OptLTP: ₹{ltp:.2f} > prev_avg ₹{self.opt_prev_avg_ltp:.2f} ({'+' if opt_delta>=0 else ''}{opt_delta:.2f})\033[0m")
        print(f"  \033[93m  OptLTP: ₹{ltp:.2f} > day_low ₹{self.opt_day_low:.2f} + margin {self.opt_low_margin}\033[0m")
        print(f"  \033[93m  OptVol: {self.opt_vol:,.0f} > prev_avg {self.opt_prev_avg_vol:,.0f}\033[0m")
        print(f"  \033[93m  FutLTP: ₹{self.fut_ltp:.2f} {fut_dir} prev_avg ₹{self.fut_prev_avg_ltp:.2f} ({'+' if fut_delta>=0 else ''}{fut_delta:.2f})\033[0m")
        if self.side == "CALL":
            print(f"  \033[93m  FutLTP: ₹{self.fut_ltp:.2f} > day_low ₹{self.fut_day_low:.2f} + margin {self.fut_low_margin}\033[0m")
        else:
            print(f"  \033[93m  FutLTP: ₹{self.fut_ltp:.2f} < day_high ₹{self.fut_day_high:.2f} - margin {self.fut_low_margin}\033[0m")
        if self.fut_prev_close:
            fut_from_close = self.fut_ltp - self.fut_prev_close
            print(f"  \033[93m  FutFromClose: {'+' if fut_from_close>=0 else ''}{fut_from_close:.2f} (limit ±{self.fut_move_limit})\033[0m")
        rally_str = f"{(ltp-self.opt_day_open)/self.opt_day_open*100:+.1f}% from open" if self.opt_day_open and self.opt_day_open > 0 else "n/a"
        atp_str = f"{(ltp-self.opt_atp)/self.opt_atp*100:+.1f}% from ATP ₹{self.opt_atp:.1f}" if self.opt_atp and self.opt_atp > 0 else "n/a"
        bs_str = f"{(self.opt_tbq or 0)/(self.opt_tsq):.2f}" if self.opt_tsq and self.opt_tsq > 0 else "n/a"
        print(f"  \033[93m  Rally: {rally_str} │ ATP: {atp_str} │ B/S: {bs_str}\033[0m")
        if self.opt_prev_min_open is not None:
            rng = self.opt_prev_min_high - self.opt_prev_min_low
            print(f"  \033[93m  PrevMin: O={self.opt_prev_min_open:.2f} H={self.opt_prev_min_high:.2f} L={self.opt_prev_min_low:.2f} C={self.opt_prev_min_close:.2f} Avg={self.opt_prev_avg_ltp:.2f} Range={rng:.2f}\033[0m")
        print(f"  \033[93m  Trail: ₹{trail:.2f} ({trail/ltp*100:.1f}%) │ Stop: ₹{self.stop_loss:.2f}\033[0m")
        if self.opt_delta is not None:
            rs_for_bev = ltp * 0.02
            fut_pts_needed = rs_for_bev / abs(self.opt_delta) if self.opt_delta != 0 else 999
            gk = []
            if self.opt_delta is not None: gk.append(f"Δ={self.opt_delta:+.3f}")
            if self.opt_theta is not None: gk.append(f"θ={self.opt_theta:.1f}")
            if self.opt_gamma is not None: gk.append(f"γ={self.opt_gamma:.5f}")
            if self.opt_vega is not None: gk.append(f"ν={self.opt_vega:.1f}")
            if self.opt_iv is not None: gk.append(f"IV={self.opt_iv:.1f}%")
            print(f"  \033[93m  Greeks: {' '.join(gk)}\033[0m")
            print(f"  \033[93m  BEV needs: ₹{rs_for_bev:.1f} (+2%) → Fut needs {fut_pts_needed:+.0f}pts (at Δ={abs(self.opt_delta):.3f})\033[0m")

        return "BUY"
    def on_future_tick(self, ltp, timestamp, feed_data=None):
        ltp = float(ltp)
        self.fut_ltp = ltp
        if self.fut_day_low is None or ltp < self.fut_day_low:
            self.fut_day_low = ltp
        if self.fut_day_high is None or ltp > self.fut_day_high:
            self.fut_day_high = ltp
        # Use real market OHLC day low/high if available
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
        minute_key = self._get_minute_key(timestamp)
        self._roll_minute(minute_key)
        self._min_fut_ltps.append(ltp)
    def on_tick(self, ltp, timestamp, feed_data=None):
        return self.on_option_tick(ltp, timestamp, feed_data)
    def configure(self):
        print(f"\n  Option + Future Momentum ({self.side}) Parameters:")
        print(f"    opt_low_margin:  Option LTP must be > day_low + this to buy")
        if self.side == "CALL":
            print(f"    fut_low_margin:  Future LTP must be > day_low + this (rising)")
        else:
            print(f"    fut_low_margin:  Future LTP must be < day_high − this (falling)")
        print(f"\n    Trailing stop modes:")
        print(f"      1. Percentage (% of entry price) — auto-scales to option size")
        print(f"      2. Fixed points — same trail regardless of option price")
        mode_pick = input(f"    Trail mode [1]: ").strip()
        if mode_pick == "2":
            self.trail_mode = "pts"
            self.trail_tiers = [(0, 10), (30, 15), (50, 20), (70, 25), (90, 30)]
            print(f"    ✓ Fixed-point mode")
        else:
            self.trail_mode = "pct"
            self.trail_tiers = [(0, 1), (2, 1), (6, 2), (15, 5)]
            print(f"    ✓ Percentage mode")
        print(f"\n    Current trailing stop tiers:")
        if self.trail_mode == "pct":
            for gain_pct, trail_pct in self.trail_tiers:
                print(f"      gain >= {gain_pct:>3}%: trail = {trail_pct}% of entry  (e.g. ₹{100*trail_pct/100:.0f} on ₹100 option)")
        else:
            for gain, trail in self.trail_tiers:
                locked = gain - trail if gain > 0 else f"max loss {trail}"
                print(f"      gain >= {gain:>3}: trail = {trail} pts  (locks {locked} pts)")
        val = input(f"\n  Option day-low margin [{self.opt_low_margin}]: ").strip()
        if val:

            try:

                self.opt_low_margin = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        val = input(f"  Future day-low margin [{self.fut_low_margin}]: ").strip()
        if val:

            try:

                self.fut_low_margin = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        edit_tiers = input(f"  Edit trail tiers? [n]: ").strip().lower()
        if edit_tiers == "y":
            if self.trail_mode == "pct":
                print(f"  Enter tiers as: gain%:trail%, ... Example: 0:8, 10:6, 20:5, 30:4")
            else:
                print(f"  Enter tiers as: gain:trail, ... Example: 0:10, 30:15, 50:20, 70:25, 90:30")
            raw = input(f"  Tiers [{', '.join(f'{g}:{t}' for g,t in self.trail_tiers)}]: ").strip()
            if raw:
                try:
                    tiers = []
                    for pair in raw.split(","):
                        g, t = pair.strip().split(":")
                        tiers.append((float(g), float(t)))
                    tiers.sort(key=lambda x: x[0])
                    self.trail_tiers = tiers
                    print(f"  ✓ Updated trail tiers")
                except Exception as e:
                    print(f"  Invalid format, keeping defaults: {e}")
        print(f"\n    Breakeven lock (prevents winners→losers):")
        print(f"      BEV trigger: {self.breakeven_trigger_pct}% (0 = activate on ANY increase)")
        print(f"      BEV floor: entry + {'₹' if self.bev_buffer_mode=='rupees' else ''}{self.breakeven_buffer}{'%' if self.bev_buffer_mode=='pct' else ''}")
        val = input(f"  Breakeven trigger % [{self.breakeven_trigger_pct}]: ").strip()
        if val:

            try:

                self.breakeven_trigger_pct = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        mode = input(f"  BEV buffer mode (rupees/pct) [{self.bev_buffer_mode}]: ").strip().lower()
        if mode in ("rupees", "pct"): self.bev_buffer_mode = mode
        buf_label = "₹" if self.bev_buffer_mode == "rupees" else "%"
        val = input(f"  Breakeven buffer {buf_label} [{self.breakeven_buffer}]: ").strip()
        if val:

            try:

                self.breakeven_buffer = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        print(f"  ✓ BEV: trigger={self.breakeven_trigger_pct}%, buffer={self.breakeven_buffer}{buf_label}")
        print(f"  ✓ Set: opt_margin={self.opt_low_margin}, fut_margin={self.fut_low_margin}")
        # Straight-down wider stop
        print(f"\n    Straight-down stop (ONLY when price goes straight down):")
        print(f"      Active when peak never exceeds entry + {self.breakeven_trigger_pct}%")
        print(f"      Uses wider stop of {self.straight_down_stop_pct}%")
        val = input(f"  Straight-down stop % [{self.straight_down_stop_pct}]: ").strip()
        if val:

            try:

                self.straight_down_stop_pct = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        print(f"  ✓ Straight-down stop: {self.straight_down_stop_pct}% (until +{self.breakeven_trigger_pct}% BEV activates)")
        # Future-from-close filter
        print(f"\n    Future-from-close filter (avoid buying pumped options):")
        print(f"      CALL blocked when future > prev_close + limit")
        print(f"      PUT  blocked when future < prev_close - limit")
        print(f"      Set to 0 to disable")
        print(f"      Current: {self.fut_move_limit} pts")
        val = input(f"  Edit fut move limit? [{self.fut_move_limit}]: ").strip()
        if val:

            try:

                self.fut_move_limit = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        print(f"  ✓ Future move limit: {self.fut_move_limit} pts")
        # Entry filters: avoid buying at the high
        print(f"\n    Entry filters (avoid buying pumped options):")
        print(f"      Open rally: skip if option rallied >{self.opt_open_rally_pct}% from today's open (0=off)")
        print(f"      ATP distance: skip if option is >{self.opt_atp_distance_pct}% above day's avg price (0=off)")
        print(f"      Buy/Sell ratio: skip if tbq/tsq < {self.opt_buy_sell_ratio} (0=off)")
        val = input(f"  Open rally max % [{self.opt_open_rally_pct}]: ").strip()
        if val:

            try:

                self.opt_open_rally_pct = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        val = input(f"  ATP distance max % [{self.opt_atp_distance_pct}]: ").strip()
        if val:

            try:

                self.opt_atp_distance_pct = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        val = input(f"  Buy/Sell ratio min [{self.opt_buy_sell_ratio}]: ").strip()
        if val:

            try:

                self.opt_buy_sell_ratio = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        print(f"  ✓ Open rally: {self.opt_open_rally_pct}% │ ATP dist: {self.opt_atp_distance_pct}% │ B/S ratio: {self.opt_buy_sell_ratio}")
        # Rally base mode
        print(f"\n    Rally base: '{self.rally_base}' (open = today's open, daylow_prevclose = min of prev close & day low)")
        val = input(f"  Rally base [{self.rally_base}]: ").strip().lower()
        if val in ("open", "daylow_prevclose"): self.rally_base = val
        print(f"  ✓ Rally base: {self.rally_base}")
        # Minute interval
        print(f"\n    Minute interval: check entry every {self.minute_interval} minute(s)")
        val = input(f"  Minute interval [{self.minute_interval}]: ").strip()
        if val:

            try:

                self.minute_interval = max(1, int(val))

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        print(f"  ✓ Minute interval: {self.minute_interval}")
        # Momentum threshold
        print(f"\n    Momentum threshold: LTP must exceed prev avg by ₹{self.opt_momentum_threshold}")
        val = input(f"  Momentum threshold ₹ [{self.opt_momentum_threshold}]: ").strip()
        if val:

            try:

                self.opt_momentum_threshold = float(val)

            except ValueError:

                print(f"  ⚠ Invalid input '{val}', keeping current value")
        print(f"  ✓ Momentum threshold: ₹{self.opt_momentum_threshold}")

        # ── OHLC & Trade Management Filters ──
        print(f"\n    ── OHLC & Trade Management Filters ──")
        print(f"    Require lower wick: {'ON' if self.require_lower_wick else 'OFF'}")
        val = input(f"  Require lower wick? (y/n) [{'y' if self.require_lower_wick else 'n'}]: ").strip().lower()
        if val == 'y': self.require_lower_wick = True
        elif val == 'n': self.require_lower_wick = False
        print(f"  ✓ Require lower wick: {'ON' if self.require_lower_wick else 'OFF'}")

        val = input(f"  Block marubozu (0=off, 0.80=block 80%+ body) [{self.block_marubozu}]: ").strip()
        if val:
            try: self.block_marubozu = float(val)
            except ValueError: print(f"  ⚠ Invalid input '{val}', keeping current value")
        print(f"  ✓ Block marubozu: {self.block_marubozu}")

        val = input(f"  Max above high Rs (0=off) [{self.max_above_high}]: ").strip()
        if val:
            try: self.max_above_high = float(val)
            except ValueError: print(f"  ⚠ Invalid input '{val}', keeping current value")

        val = input(f"  Max range Rs (0=off, NOT recommended) [{self.max_range}]: ").strip()
        if val:
            try: self.max_range = float(val)
            except ValueError: print(f"  ⚠ Invalid input '{val}', keeping current value")

        val = input(f"  Max momentum Rs (0=off) [{self.max_momentum}]: ").strip()
        if val:
            try: self.max_momentum = float(val)
            except ValueError: print(f"  ⚠ Invalid input '{val}', keeping current value")

        val = input(f"  Max hold minutes (0=off) [{self.max_hold}]: ").strip()
        if val:
            try: self.max_hold = int(val)
            except ValueError: print(f"  ⚠ Invalid input '{val}', keeping current value")

        val = input(f"  Target profit %% (0=use trail) [{self.target_profit}]: ").strip()
        if val:
            try: self.target_profit = float(val)
            except ValueError: print(f"  ⚠ Invalid input '{val}', keeping current value")

        val = input(f"  Rebuy drop %% (0=off) [{self.rebuy_drop}]: ").strip()
        if val:
            try: self.rebuy_drop = float(val)
            except ValueError: print(f"  ⚠ Invalid input '{val}', keeping current value")

        val = input(f"  First trade only? (y/n) [{'y' if self.first_only else 'n'}]: ").strip().lower()
        if val == 'y': self.first_only = True
        elif val == 'n': self.first_only = False

        active = [f for f in [
            'lower_wick' if self.require_lower_wick else None,
            f'marubozu>{self.block_marubozu}' if self.block_marubozu > 0 else None,
            f'abvHigh>{self.max_above_high}' if self.max_above_high > 0 else None,
            f'maxMom>{self.max_momentum}' if self.max_momentum > 0 else None,
            f'maxHold={self.max_hold}m' if self.max_hold > 0 else None,
            f'target={self.target_profit}%' if self.target_profit > 0 else None,
            f'rebuyDrop={self.rebuy_drop}%' if self.rebuy_drop > 0 else None,
            'firstOnly' if self.first_only else None,
        ] if f]
        print(f"  ✓ Active OHLC/mgmt filters: {', '.join(active) if active else 'none'}")
    def describe(self):
        super().describe()
        print(f"  Side: {self.side} │ opt_margin={self.opt_low_margin}, fut_margin={self.fut_low_margin}")
        mode_label = "% of entry" if self.trail_mode == "pct" else "fixed pts"
        if self.trail_mode == "pct":
            print(f"  Trail ({mode_label}): {', '.join(f'{g}%→{t}%' for g,t in self.trail_tiers)}")
        else:
            print(f"  Trail ({mode_label}): {', '.join(f'{g}→{t}' for g,t in self.trail_tiers)}")
        buf_label = "₹" if self.bev_buffer_mode == "rupees" else "%"
        print(f"  Breakeven lock: trigger={self.breakeven_trigger_pct}%, buffer={self.breakeven_buffer}{buf_label}")
        print(f"  Straight-down stop: {self.straight_down_stop_pct}% (until +{self.breakeven_trigger_pct}% BEV activates)")
        print(f"  Future-from-close: {self.fut_move_limit} pts (prev_close: {'₹'+f'{self.fut_prev_close:.2f}' if self.fut_prev_close else 'pending'})")
        print(f"  Open rally: {self.opt_open_rally_pct}% (open: {'₹'+f'{self.opt_day_open:.2f}' if self.opt_day_open else 'pending'}) │ ATP dist: {self.opt_atp_distance_pct}% │ B/S ratio: {self.opt_buy_sell_ratio}")
        if self.future_key:
            print(f"  Future: {self.future_key}")
        if self._in_position and self.stop_loss and self.entry_price:
            if self.trail_mode == "pct":
                gain = ((self.peak_price - self.entry_price) / self.entry_price) * 100
            else:
                gain = self.peak_price - self.entry_price
            trail = self._get_trail_distance(gain)
            bev_str = f", BEV=₹{self._breakeven_stop:.2f} ✓" if self._breakeven_active else ""
            print(f"  Live: entry=₹{self.entry_price:.2f}, peak=₹{self.peak_price:.2f}, "
                  f"gain={gain:.1f}{'%' if self.trail_mode=='pct' else 'pts'}, trail=₹{trail:.1f}, SL=₹{self.stop_loss:.2f}{bev_str}")
        if self.opt_prev_avg_ltp:
            fut_str = f"{self.fut_prev_avg_ltp:.2f}" if self.fut_prev_avg_ltp else "?"
            vol_str = f"{self.opt_prev_avg_vol:,.0f}" if self.opt_prev_avg_vol else "?"
            print(f"  1-min avgs: opt_ltp={self.opt_prev_avg_ltp:.2f}, fut_ltp={fut_str}, opt_vol={vol_str}")
        # New filters status
        active = [f for f in [
            'lower_wick' if self.require_lower_wick else None,
            f'marubozu>{self.block_marubozu}' if self.block_marubozu > 0 else None,
            f'maxRange>{self.max_range}' if self.max_range > 0 else None,
            f'maxMom>{self.max_momentum}' if self.max_momentum > 0 else None,
            f'abvHigh>{self.max_above_high}' if self.max_above_high > 0 else None,
            f'rebuyDrop={self.rebuy_drop}%' if self.rebuy_drop > 0 else None,
            f'maxHold={self.max_hold}m' if self.max_hold > 0 else None,
            f'target={self.target_profit}%' if self.target_profit > 0 else None,
            'firstOnly' if self.first_only else None,
        ] if f]
        if active:
            print(f"  OHLC/Mgmt filters: {', '.join(active)}")
        else:
            print(f"  OHLC/Mgmt filters: none")
STRATEGIES["2"]["class"] = OptionFutureMomentum
STRATEGIES["3"]["class"] = OptionFutureMomentum
STRATEGIES["4"]["class"] = OptionFutureMomentum
class StrategyRunner:
    def __init__(self, strategy, paper_trader, instrument_key, future_key=None):
        self.strategy = strategy
        self.trader = paper_trader
        self.instrument_key = instrument_key
        self.future_key = future_key
        self._last_status_tick = [0]
    def on_tick(self, key, ltp, timestamp, feed_data=None):
        if ltp is None: return
        try: self._process_tick(key, ltp, timestamp, feed_data)
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
            if signal == "BUY" and self.trader.can_buy(): self.trader.buy(ltp, timestamp)
            elif signal == "SELL" and self.trader.can_sell(): self.trader.sell(ltp, timestamp)
            if self.strategy.tick_count - self._last_status_tick[0] >= 200:
                self._last_status_tick[0] = self.strategy.tick_count
                print(f"  {self.trader.status_line(self.strategy)}")
                if isinstance(self.strategy, OptionFutureMomentum):
                    self._print_conditions(self.strategy)
        elif self.future_key and key == self.future_key:
            if hasattr(self.strategy, 'on_future_tick'):
                self.strategy.on_future_tick(ltp, timestamp, feed_data)
    @staticmethod
    def _print_conditions(s):
        cond_parts = []
        if s.opt_prev_avg_ltp is not None and s.opt_ltp is not None:
            ok = "✓" if s.opt_ltp > s.opt_prev_avg_ltp else "✗"
            cond_parts.append(f"{ok} OptLTP:{s.opt_ltp:.1f}>{s.opt_prev_avg_ltp:.1f}")
        if s.opt_day_low is not None and s.opt_ltp is not None:
            ok = "✓" if s.opt_ltp > s.opt_day_low + s.opt_low_margin else "✗"
            cond_parts.append(f"{ok} OptLTP>{s.opt_day_low:.1f}+{s.opt_low_margin}")
        if s.opt_prev_avg_vol is not None and s.opt_vol is not None:
            ok = "✓" if s.opt_vol > s.opt_prev_avg_vol else "✗"
            cond_parts.append(f"{ok} Vol:{s.opt_vol:,.0f}>{s.opt_prev_avg_vol:,.0f}")
        if s.fut_prev_avg_ltp is not None and s.fut_ltp is not None:
            if s.side == "CALL":
                ok = "✓" if s.fut_ltp > s.fut_prev_avg_ltp else "✗"
                cond_parts.append(f"{ok} FutLTP:{s.fut_ltp:.1f}>{s.fut_prev_avg_ltp:.1f}")
            else:
                ok = "✓" if s.fut_ltp < s.fut_prev_avg_ltp else "✗"
                cond_parts.append(f"{ok} FutLTP:{s.fut_ltp:.1f}<{s.fut_prev_avg_ltp:.1f}")
        if s.side == "CALL" and s.fut_day_low is not None and s.fut_ltp is not None:
            ok = "✓" if s.fut_ltp > s.fut_day_low + s.fut_low_margin else "✗"
            cond_parts.append(f"{ok} FutLTP>{s.fut_day_low:.1f}+{s.fut_low_margin}")
        elif s.side == "PUT" and s.fut_day_high is not None and s.fut_ltp is not None:
            ok = "✓" if s.fut_ltp < s.fut_day_high - s.fut_low_margin else "✗"
            cond_parts.append(f"{ok} FutLTP<{s.fut_day_high:.1f}-{s.fut_low_margin}")
        if cond_parts:
            print(f"  \033[90m  [{s.side}] {' │ '.join(cond_parts)}\033[0m")
    def print_final_summary(self):
        print(f"\n  {'═'*60}")
        print(f"  STRATEGY SUMMARY — {self.strategy.name}")
        print(f"  {'═'*60}")
        self.strategy.describe()
        print(f"  Ticks processed: {self.strategy.tick_count:,}")
        self.trader.print_trade_table()
        if self.trader.in_position:
            ur_pnl, ur_pct = self.trader.unrealized_pnl()
            sign = "+" if ur_pnl >= 0 else ""
            print(f"  ⚠ OPEN POSITION: Entry ₹{self.trader.entry_price:.2f}, LTP ₹{self.trader.current_ltp:.2f}, Unrealized {sign}₹{ur_pnl:.2f} ({sign}{ur_pct:.1f}%)")
        print(f"  {'═'*60}")
class DualStrategyRunner:
    def __init__(self, call_strategy, call_trader, call_key, put_strategy, put_trader, put_key, future_key):
        self.call_strat = call_strategy
        self.call_trader = call_trader
        self.call_key = call_key
        self.put_strat = put_strategy
        self.put_trader = put_trader
        self.put_key = put_key
        self.future_key = future_key
        self._last_call_status = [0]
        self._last_put_status = [0]
        self.strategy = call_strategy
        self.trader = call_trader
        self.instrument_key = call_key
    def on_tick(self, key, ltp, timestamp, feed_data=None):
        if ltp is None: return
        try: self._process_tick(key, ltp, timestamp, feed_data)
        except Exception as e:
            import traceback
            print(f"  \033[91m[DUAL STRATEGY ERROR] {e}\033[0m")
            traceback.print_exc()
    def _process_tick(self, key, ltp, timestamp, feed_data=None):
        if key == self.call_key:
            self.call_trader.update_tick(ltp, timestamp)
            signal = self.call_strat.on_option_tick(ltp, timestamp, feed_data)
            if signal == "BUY" and self.call_trader.can_buy(): self.call_trader.buy(ltp, timestamp)
            elif signal == "SELL" and self.call_trader.can_sell(): self.call_trader.sell(ltp, timestamp)
            if self.call_strat.tick_count - self._last_call_status[0] >= 200:
                self._last_call_status[0] = self.call_strat.tick_count
                print(f"  \033[36m[CE] {self.call_trader.status_line(self.call_strat)}\033[0m")
                StrategyRunner._print_conditions(self.call_strat)
        elif key == self.put_key:
            self.put_trader.update_tick(ltp, timestamp)
            signal = self.put_strat.on_option_tick(ltp, timestamp, feed_data)
            if signal == "BUY" and self.put_trader.can_buy(): self.put_trader.buy(ltp, timestamp)
            elif signal == "SELL" and self.put_trader.can_sell(): self.put_trader.sell(ltp, timestamp)
            if self.put_strat.tick_count - self._last_put_status[0] >= 200:
                self._last_put_status[0] = self.put_strat.tick_count
                print(f"  \033[35m[PE] {self.put_trader.status_line(self.put_strat)}\033[0m")
                StrategyRunner._print_conditions(self.put_strat)
        elif key == self.future_key:
            self.call_strat.on_future_tick(ltp, timestamp, feed_data)
            self.put_strat.on_future_tick(ltp, timestamp, feed_data)
    def print_final_summary(self):
        for label, strat, trader in [("CALL (CE)", self.call_strat, self.call_trader),
                                     ("PUT (PE)", self.put_strat, self.put_trader)]:
            print(f"\n  {'═'*60}")
            print(f"  {label} SUMMARY — {strat.name}")
            print(f"  {'═'*60}")
            strat.describe()
            print(f"  Ticks processed: {strat.tick_count:,}")
            trader.print_trade_table()
            if trader.in_position:
                ur_pnl, ur_pct = trader.unrealized_pnl()
                sign = "+" if ur_pnl >= 0 else ""
                print(f"  ⚠ OPEN POSITION: Entry ₹{trader.entry_price:.2f}, LTP ₹{trader.current_ltp:.2f}, Unrealized {sign}₹{ur_pnl:.2f} ({sign}{ur_pct:.1f}%)")
            print(f"  {'═'*60}")
# ═══════════════════════════════════════════════════════════════
# INTERACTIVE MENU
# ═══════════════════════════════════════════════════════════════
def pick_index():
    print("\n  Indices:")
    names = list(INDICES.keys())
    for i, name in enumerate(names, 1):
        print(f"    {i}. {name:>12}  →  {INDICES[name]}")
    pick = input(f"\n  Pick [1-{len(names)}]: ").strip()
    try:
        idx = int(pick) - 1
        if 0 <= idx < len(names):
            return names[idx], INDICES[names[idx]]
    except ValueError:
        lower = pick.lower()
        if lower in INDICES:
            return lower, INDICES[lower]
    print("  Invalid choice.")
    return None, None
def add_index_options(client, sub_keys, sub_meta):
    idx_name, key = pick_index()
    if not key: return
    print(f"\n  Fetching expiries for {idx_name}...")
    expiries = get_expiries(client, key)
    if not expiries:
        print("  No expiries found.")
        return
    print(f"\n  Expiries:")
    for i, e in enumerate(expiries[:15], 1):
        print(f"    {i}. {e}")
    pick = input(f"\n  Pick expiry [1]: ").strip() or "1"
    try: expiry = expiries[int(pick) - 1]
    except (ValueError, IndexError): expiry = expiries[0]
    print(f"\n  Fetching option chain: {idx_name} / {expiry}...")
    chain = get_option_chain(client, key, expiry)
    if not chain:
        print("  No chain data.")
        return
    spot = chain[0].get("spot", 0) if chain else 0
    picks = pick_options_from_chain(chain, spot)
    if picks:
        for inst_key, side, row in picks:
            sub_keys.append(inst_key)
            strike = row.get("strike", "?")
            ltp = row.get("call_ltp") if side == "CE" else row.get("put_ltp")
            display_name = f"{idx_name.upper()} {int(strike)}{side}"
            sub_meta[inst_key] = {"name": display_name, "ltp": ltp, "expiry": expiry}  # ◀ FIXED: stores expiry
            ltp_str = f"  ₹{ltp}" if ltp else ""
            print(f"  ✓ {display_name:<25} {inst_key:<20}{ltp_str}")
        print(f"  Added {len(picks)} option contracts")
    else:
        print("  Nothing selected.")
def add_index_for_streaming(client, sub_keys, sub_meta):
    name, key = pick_index()
    if key:
        sub_keys.append(key)
        ltps = fetch_ltp(client, [key])
        ltp = ltps.get(key)
        sub_meta[key] = {"name": name.upper(), "ltp": ltp}
        ltp_str = f"  ₹{ltp}" if ltp else ""
        print(f"  ✓ {name.upper():<25} {key:<20}{ltp_str}")
def add_futures(client, sub_keys, sub_meta):
    name, key = pick_index()
    if not key: return
    print(f"\n  Searching futures for {name}...")
    api = upstox_client.InstrumentsApi(client)
    try:
        resp = api.search_instrument(name)
        data = resp.data if hasattr(resp, "data") else []
        results = [r.to_dict() if hasattr(r, "to_dict") else r for r in data]
    except Exception as e:
        print(f"  Search error: {e}")
        return
    futures = [r for r in results if "FUT" in (r.get("instrument_type") or "").upper()]
    if not futures:
        print(f"  No futures found for {name}.")
        return
    print(f"\n  Futures contracts for {name}:")
    for i, r in enumerate(futures[:10], 1):
        ikey = r.get("instrument_key") or "?"
        rname = r.get("name") or r.get("trading_symbol") or ""
        expiry = clean_expiry(r.get("expiry") or "")
        print(f"    {i}. {ikey:<35} {rname:<30} exp:{expiry}")
    picks = input("\n  Pick numbers (comma-sep) or 'all': ").strip()
    if not picks: return
    added = futures[:10] if picks.lower() == "all" else []
    if not added:
        for p in picks.split(","):
            try: added.append(futures[int(p.strip()) - 1])
            except (ValueError, IndexError): print(f"  [?] Invalid: {p}")
    for r in added:
        k = r.get("instrument_key")
        if k:
            sub_keys.append(k)
            expiry = clean_expiry(r.get("expiry") or "")
            display_name = f"{name.upper()} FUT {expiry}"
            sub_meta[k] = {"name": display_name, "ltp": None, "expiry": expiry}  # ◀ FIXED: stores expiry
            print(f"  ✓ {display_name:<25} {k}")
def add_stock(client, sub_keys, sub_meta):
    query = input("\n  Stock name (e.g. RELIANCE): ").strip()
    if not query: return
    print(f"  Searching '{query}'...")
    results = search_instrument(client, query)
    if not results:
        print("  No results.")
        return
    print(f"\n  Found {len(results)} results:")
    for i, r in enumerate(results[:15], 1):
        ikey = r.get("instrument_key") or "?"
        rname = r.get("name") or r.get("trading_symbol") or ""
        exchange = r.get("exchange") or ""
        itype = r.get("instrument_type") or ""
        print(f"    {i}. {ikey:<35} {rname:<30} {exchange} {itype}")
    picks = input("\n  Pick numbers (comma-sep) or 'all': ").strip()
    if not picks: return
    added = []
    if picks.lower() == "all":
        for r in results[:15]:
            k = r.get("instrument_key") or r.get("key")
            if k: added.append((k, r))
    else:
        for p in picks.split(","):
            try:
                r = results[int(p.strip()) - 1]
                k = r.get("instrument_key") or r.get("key")
                if k: added.append((k, r))
            except (ValueError, IndexError): print(f"  [?] Invalid: {p}")
    if not added: return
    ltps = fetch_ltp(client, [k for k, _ in added])
    for k, r in added:
        sub_keys.append(k)
        rname = r.get("name") or r.get("trading_symbol") or k.split("|")[-1]
        ltp = ltps.get(k)
        sub_meta[k] = {"name": rname, "ltp": ltp}
        ltp_str = f"  ₹{ltp}" if ltp else ""
        print(f"  ✓ {rname:<25} {k:<20}{ltp_str}")
def add_custom_key(client, sub_keys, sub_meta):
    print("\n  Enter instrument keys (comma-separated):")
    raw = input("  > ").strip()
    if not raw: return
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    ltps = fetch_ltp(client, keys)
    for k in keys:
        sub_keys.append(k)
        ltp = ltps.get(k)
        symbol = k.split("|")[-1] if "|" in k else k
        sub_meta[k] = {"name": symbol, "ltp": ltp}
        ltp_str = f"  ₹{ltp}" if ltp else ""
        print(f"  ✓ {symbol:<25} {k:<20}{ltp_str}")
def _pick_mcx_commodity():
    print("\n  MCX Commodities (9:00 AM — 11:30 PM IST):")
    names = list(MCX_COMMODITIES.keys())
    for i, name in enumerate(names, 1):
        print(f"    {i}. {name.upper()}")
    pick = input(f"\n  Pick [1-{len(names)}]: ").strip()
    try:
        idx = int(pick) - 1
        if 0 <= idx < len(names):
            return names[idx], MCX_COMMODITIES[names[idx]]
    except ValueError:
        commodity = pick.lower()
        if commodity in MCX_COMMODITIES:
            return commodity, MCX_COMMODITIES[commodity]
    print("  Invalid.")
    return None, None
_mcx_instruments_cache = None
def _download_mcx_instruments():
    global _mcx_instruments_cache
    if _mcx_instruments_cache is not None:
        return _mcx_instruments_cache
    import urllib.request
    import gzip
    url = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz"
    print(f"  Downloading MCX instruments list...")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            compressed = resp.read()
        decompressed = gzip.decompress(compressed)
        instruments = json.loads(decompressed)
        if isinstance(instruments, list):
            _mcx_instruments_cache = instruments
        elif isinstance(instruments, dict):
            _mcx_instruments_cache = list(instruments.values()) if instruments else []
        else:
            _mcx_instruments_cache = []
        print(f"  ✓ Loaded {len(_mcx_instruments_cache)} MCX instruments")
        return _mcx_instruments_cache
    except Exception as e:
        print(f"  ✗ Download failed: {e}")
        return []
def _filter_mcx(instruments, commodity, instrument_types=None):
    results = []
    commodity_upper = commodity.upper()
    commodity_compact = commodity_upper.replace(" ", "")
    for inst in instruments:
        name = (inst.get("name") or inst.get("short_name") or "").upper().strip()
        itype = (inst.get("instrument_type") or "").upper()
        ikey = inst.get("instrument_key") or ""
        tsym = (inst.get("trading_symbol") or "").upper()
        if not ikey.startswith("MCX_FO"):
            continue
        if tsym:
            if tsym.startswith(commodity_compact):
                suffix = tsym[len(commodity_compact):]
                if suffix and suffix[0].isalpha():
                    continue
            else:
                continue
        else:
            if name != commodity_upper:
                continue
        if instrument_types and itype not in instrument_types:
            continue
        expiry = clean_expiry(inst.get("expiry"))
        strike = inst.get("strike_price") or inst.get("strike") or inst.get("option_strike_price") or inst.get("strikeprice") or 0
        lot = inst.get("lot_size") or ""
        tick = inst.get("tick_size") or ""
        results.append({
            "key": ikey, "name": name, "type": itype,
            "strike": float(strike) if strike else 0,
            "expiry": expiry or "", "lot": lot, "tsym": tsym, "tick": tick,
        })
    return results
def _mcx_debug_fields(instruments, commodity):
    commodity_upper = commodity.upper()
    for inst in instruments:
        name = (inst.get("name") or "").upper().strip()
        if name == commodity_upper:
            print(f"\n  [DEBUG] Sample MCX {commodity} instrument fields:")
            for k, v in inst.items():
                print(f"    {k}: {v}")
            return
def add_mcx_commodity(client, sub_keys, sub_meta):
    commodity, search_term = _pick_mcx_commodity()
    if not search_term:
        return
    print(f"\n  What to add for {search_term}?")
    print(f"    1. Futures")
    print(f"    2. Options (CE/PE)")
    choice = input(f"\n  Pick [1-2]: ").strip()
    if choice == "1":
        _add_mcx_futures(client, sub_keys, sub_meta, search_term)
    elif choice == "2":
        _add_mcx_options(client, sub_keys, sub_meta, search_term)
    else:
        print("  Invalid.")
def _add_mcx_futures(client, sub_keys, sub_meta, search_term):
    instruments = _download_mcx_instruments()
    if not instruments:
        return
    futures = _filter_mcx(instruments, search_term, instrument_types=["FUT", "FUTCOM"])
    if not futures:
        print(f"  No MCX futures found for {search_term}.")
        _mcx_debug_fields(instruments, search_term)
        all_names = set()
        for inst in instruments:
            n = (inst.get("name") or "").upper().strip()
            if search_term.upper()[:4] in n:
                all_names.add(n)
        if all_names:
            print(f"  Similar names found: {', '.join(sorted(all_names))}")
        return
    futures.sort(key=lambda x: x["expiry"])
    print(f"\n  MCX {search_term} Futures:")
    for i, f in enumerate(futures[:10], 1):
        print(f"    {i}. {f['tsym']:<30} exp:{f['expiry']}  lot:{f['lot']}  {f['key']}")
    picks = input("\n  Pick numbers (comma-sep) or 'all': ").strip()
    if not picks:
        return
    added = futures[:10] if picks.lower() == "all" else []
    if not added:
        for p in picks.split(","):
            try:
                added.append(futures[int(p.strip()) - 1])
            except (ValueError, IndexError):
                print(f"  [?] Invalid: {p}")
    for f in added:
        k = f["key"]
        sub_keys.append(k)
        display_name = f"{search_term} FUT {f['expiry']}"
        sub_meta[k] = {"name": display_name, "ltp": None, "expiry": f["expiry"]}  # ◀ FIXED: stores expiry
        print(f"  ✓ {display_name:<30} {k}")
def _add_mcx_options(client, sub_keys, sub_meta, search_term):
    from datetime import datetime as dt
    instruments = _download_mcx_instruments()
    if not instruments:
        return
    options = _filter_mcx(instruments, search_term, instrument_types=["CE", "PE"])
    if not options:
        print(f"  No MCX options found for {search_term}.")
        _mcx_debug_fields(instruments, search_term)
        all_names = set()
        for inst in instruments:
            n = (inst.get("name") or "").upper().strip()
            if search_term.upper()[:4] in n:
                all_names.add(n)
        if all_names:
            print(f"  Similar names in MCX: {', '.join(sorted(all_names))}")
        return
    expiries = sorted(set(o["expiry"] for o in options if o["expiry"]))
    today = date.today()
    print(f"\n  MCX {search_term} Options — {len(expiries)} expiries available:")
    for i, e in enumerate(expiries[:8], 1):
        try:
            exp_date = dt.strptime(e, "%Y-%m-%d").date()
            days = (exp_date - today).days
            day_name = exp_date.strftime("%a %d %b")
            label = f"{day_name} ({days}d)"
        except Exception:
            label = e
        n_ce = sum(1 for o in options if o["expiry"] == e and o["type"] == "CE")
        n_pe = sum(1 for o in options if o["expiry"] == e and o["type"] == "PE")
        print(f"    {i}. {label:<22} {n_ce} CE, {n_pe} PE")
    pick = input(f"\n  Pick expiry [1 = nearest]: ").strip() or "1"
    try:
        selected_expiry = expiries[int(pick) - 1]
    except (ValueError, IndexError):
        selected_expiry = expiries[0]
    try:
        exp_date = dt.strptime(selected_expiry, "%Y-%m-%d").date()
        exp_label = exp_date.strftime("%a %d %b %Y")
    except Exception:
        exp_label = selected_expiry
    spot = 0
    try:
        futs = _filter_mcx(instruments, search_term, instrument_types=["FUT", "FUTCOM"])
        if futs:
            futs.sort(key=lambda x: x["expiry"])
            ltps = fetch_ltp(client, [futs[0]["key"]])
            spot = ltps.get(futs[0]["key"]) or 0
    except Exception:
        pass
    exp_options = [o for o in options if o["expiry"] == selected_expiry]
    strikes_map = {}
    for o in exp_options:
        s = o["strike"]
        if s not in strikes_map:
            strikes_map[s] = {
                "strike": s, "call_key": None, "put_key": None,
                "call_ltp": None, "put_ltp": None, "call_iv": None, "put_iv": None,
                "call_delta": None, "put_delta": None, "call_oi": None, "put_oi": None,
                "spot": spot or None,
            }
        if o["type"] == "CE":
            strikes_map[s]["call_key"] = o["key"]
        else:
            strikes_map[s]["put_key"] = o["key"]
    chain = sorted(strikes_map.values(), key=lambda x: x["strike"])
    if not chain:
        print("  No strikes found.")
        return
    if spot:
        atm_idx = min(range(len(chain)), key=lambda i: abs(chain[i]["strike"] - spot))
    else:
        atm_idx = len(chain) // 2
        spot = chain[atm_idx]["strike"]
    lo = max(0, atm_idx - 10)
    hi = min(len(chain), atm_idx + 11)
    ltp_keys = []
    for i in range(lo, hi):
        if chain[i].get("call_key"): ltp_keys.append(chain[i]["call_key"])
        if chain[i].get("put_key"): ltp_keys.append(chain[i]["put_key"])
    if ltp_keys:
        try:
            ltps = fetch_ltp(client, ltp_keys)
            for i in range(lo, hi):
                if chain[i].get("call_key"): chain[i]["call_ltp"] = ltps.get(chain[i]["call_key"])
                if chain[i].get("put_key"): chain[i]["put_ltp"] = ltps.get(chain[i]["put_key"])
        except Exception:
            pass
    print(f"\n  MCX {search_term} — {exp_label}")
    visible_chain = chain[lo:hi]
    selected = pick_options_from_chain(visible_chain, spot)
    if not selected:
        print("  No options selected.")
        return
    for key, side, row in selected:
        sub_keys.append(key)
        strike = row["strike"]
        display_name = f"{search_term} {int(strike)}{side}"
        sub_meta[key] = {"name": display_name, "ltp": row.get(f"{'call' if side=='CE' else 'put'}_ltp"), "expiry": selected_expiry}  # ◀ FIXED: stores expiry
        print(f"  ✓ {display_name:<30} {key}")
    print(f"  Added {len(selected)} MCX option contracts")
def add_currency(client, sub_keys, sub_meta):
    print("\n  Currency Derivatives (9:00 AM — 5:00 PM IST):")
    names = list(CURRENCY_SEARCH.keys())
    for i, name in enumerate(names, 1):
        print(f"    {i}. {name.upper()}")
    pick = input(f"\n  Pick [1-{len(names)}]: ").strip()
    try:
        idx = int(pick) - 1
        if not (0 <= idx < len(names)):
            print("  Invalid.")
            return
        currency = names[idx]
    except ValueError:
        currency = pick.lower()
        if currency not in CURRENCY_SEARCH:
            print("  Invalid.")
            return
    search_term = CURRENCY_SEARCH[currency]
    print(f"\n  Searching currency futures for {search_term}...")
    results = search_instrument(client, search_term)
    futures = []
    for r in results:
        itype = (r.get("instrument_type") or "").upper()
        ikey = r.get("instrument_key") or ""
        iname = (r.get("name") or r.get("trading_symbol") or "").upper()
        if "FUT" not in itype:
            continue
        expiry = clean_expiry(r.get("expiry") or "")
        exchange = r.get("exchange") or ""
        futures.append({"key": ikey, "name": iname, "expiry": expiry, "exchange": exchange})
    if not futures:
        print(f"  No currency futures found for {search_term}.")
        return
    futures.sort(key=lambda x: x["expiry"])
    print(f"\n  {search_term} Futures:")
    for i, f in enumerate(futures[:10], 1):
        print(f"    {i}. {f['key']:<30} {f['name']:<15} exp:{f['expiry']} {f['exchange']}")
    picks = input("\n  Pick numbers (comma-sep) or 'all': ").strip()
    if not picks:
        return
    added = futures[:6] if picks.lower() == "all" else []
    if not added:
        for p in picks.split(","):
            try:
                added.append(futures[int(p.strip()) - 1])
            except (ValueError, IndexError):
                print(f"  [?] Invalid: {p}")
    for f in added:
        k = f["key"]
        sub_keys.append(k)
        display_name = f"{search_term} FUT {f['expiry']}"
        sub_meta[k] = {"name": display_name, "ltp": None, "expiry": f["expiry"]}  # ◀ FIXED: stores expiry
        print(f"  ✓ {display_name:<25} {k}")
# ═══════════════════════════════════════════════════════════════
# HELPERS + MAIN
# ═══════════════════════════════════════════════════════════════
def _find_future(underlying, inst_key, sub_keys, sub_meta, client):
    found_futures = [(k, sub_meta.get(k, {}).get("name", "")) for k in sub_keys if "FUT" in sub_meta.get(k, {}).get("name", "").upper() and underlying.upper() in sub_meta.get(k, {}).get("name", "").upper()]
    if found_futures:
        print(f"  Found matching future(s):")
        for i, (fk, fn) in enumerate(found_futures, 1):
            print(f"    {i}. {fn:<30} {fk}")
        fpick = input(f"  Pick future [1]: ").strip() or "1"
        try:
            fkey = found_futures[int(fpick) - 1][0]
            print(f"  ✓ Using future: {found_futures[int(fpick) - 1][1]}")
            return fkey
        except (ValueError, IndexError):
            return found_futures[0][0]
    print(f"  No matching future in subscription. Searching for {underlying} futures...")
    if not client: return None
    try:
        api = upstox_client.InstrumentsApi(client)
        resp = api.search_instrument(underlying)
        data = resp.data if hasattr(resp, "data") else []
        results = [r.to_dict() if hasattr(r, "to_dict") else r for r in data]
        exchange = inst_key.split("_")[0]
        futures = [{"key": r.get("instrument_key"), "expiry": clean_expiry(r.get("expiry") or "")} for r in results if "FUT" in (r.get("instrument_type") or "").upper() and (r.get("instrument_key") or "").startswith(f"{exchange}_FO")]
        if futures:
            futures.sort(key=lambda x: x["expiry"] or "")
            fut = futures[0]
            fut_display = f"{underlying} FUT {fut['expiry']}"
            if fut["key"] not in sub_keys:
                sub_keys.append(fut["key"])
                sub_meta[fut["key"]] = {"name": fut_display, "ltp": None, "expiry": fut["expiry"]}
                print(f"  ✓ Auto-subscribed: {fut_display} ({fut['key']})")
            return fut["key"]
    except Exception as e:
        print(f"  ⚠ Future search failed: {e}")
    return None
def _find_counterpart_option(inst_name, inst_key, sub_keys, sub_meta, client):
    if "CE" in inst_name.upper():
        target_side, source_side = "PE", "CE"
    elif "PE" in inst_name.upper():
        target_side, source_side = "CE", "PE"
    else:
        return None, None
    target_name = inst_name.upper().replace(source_side, target_side)
    for k in sub_keys:
        if sub_meta.get(k, {}).get("name", "").upper() == target_name:
            print(f"  ✓ {target_side} already subscribed: {sub_meta[k]['name']}")
            return k, sub_meta[k]["name"]
    if client:
        try:
            parts = inst_name.split()
            underlying = parts[0] if parts else ""
            strike_side = parts[1] if len(parts) > 1 else ""
            strike = strike_side.replace("CE", "").replace("PE", "").replace("ce", "").replace("pe", "")
            api = upstox_client.InstrumentsApi(client)
            resp = api.search_instrument(f"{underlying} {strike}")
            data = resp.data if hasattr(resp, "data") else []
            results = [r.to_dict() if hasattr(r, "to_dict") else r for r in data]
            exchange = inst_key.split("_")[0]
            # Carry over the expiry from the source option
            source_expiry = sub_meta.get(inst_key, {}).get("expiry", "")
            for r in results:
                ikey = r.get("instrument_key") or ""
                itype = (r.get("instrument_type") or "").upper()
                tsym = (r.get("trading_symbol") or "").upper()
                if not ikey.startswith(f"{exchange}_FO"): continue
                if target_side not in itype and target_side not in tsym: continue
                if strike not in tsym: continue
                display = target_name
                if ikey not in sub_keys:
                    sub_keys.append(ikey)
                    sub_meta[ikey] = {"name": display, "ltp": None, "expiry": source_expiry}
                    print(f"  ✓ Auto-subscribed {target_side}: {display} ({ikey})")
                return ikey, display
        except Exception as e:
            print(f"  ⚠ {target_side} search failed: {e}")
    return None, None
def setup_strategy(sub_keys, sub_meta, client=None):
    print(f"\n  ── Available Strategies ──")
    for sid, info in STRATEGIES.items():
        print(f"    {sid}. {info['name']}")
    print(f"    0. No strategy (just stream)")
    pick = input(f"\n  Pick strategy: ").strip()
    if pick == "0" or pick not in STRATEGIES: return None
    is_dual = (pick == "3")
    is_single_momentum = pick in ("2", "4")
    if pick == "1":
        strategy = STRATEGIES[pick]["class"]()
        strategy.configure()
    elif is_single_momentum or is_dual:
        side = "PUT" if pick == "4" else "CALL"
        label = "PE only" if pick == "4" else ("CE only" if pick == "2" else "Dual CE+PE")
        print(f"\n  {label} Momentum — configuring parameters.")
        temp = OptionFutureMomentum(side=side)
        # Auto-detect fut_move_limit based on underlying
        ul = underlying.upper() if underlying else ""
        if "BANKNIFTY" in ul or "BANKEX" in ul:
            temp.fut_move_limit = 200
        elif "NIFTY" in ul or "SENSEX" in ul or "FINNIFTY" in ul or "MIDCPNIFTY" in ul:
            temp.fut_move_limit = 75
        else:
            # Unknown instrument — ask user
            print(f"  ⚠ Unknown underlying '{underlying}' — please set future move limit manually.")
            val = input(f"  Future move limit pts [200]: ").strip()
            temp.fut_move_limit = float(val) if val else 200
        temp.configure()
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
    future_key = None
    if pick in ("2", "3", "4"):
        print(f"\n  Strategy needs the related FUTURE for {inst_name}.")
        future_key = _find_future(underlying, inst_key, sub_keys, sub_meta, client)
        if future_key: print(f"  Future key: {future_key}")
        else: print(f"  ⚠ No future found. Future conditions will be skipped.")
    lot_input = input(f"\n  Lot size [{1}]: ").strip()
    lot_size = int(lot_input) if lot_input else 1
    max_input = input(f"  Max trades per side [{50}]: ").strip()
    max_trades = int(max_input) if max_input else 50
    if not is_dual:
        if is_single_momentum:
            # Strategy 2 (CE) or 4 (PE) — use configured temp as the strategy
            strategy = temp
            strategy.future_key = future_key
        elif isinstance(strategy, OptionFutureMomentum):
            strategy.future_key = future_key
        trader = PaperTrader(inst_name, lot_size=lot_size, max_trades=max_trades)
        runner = StrategyRunner(strategy, trader, inst_key, future_key=future_key)
        print(f"\n  ✓ Strategy '{strategy.name}' on {inst_name}")
        if future_key: print(f"    Future: {sub_meta.get(future_key, {}).get('name', future_key)}")
        print(f"    Lot: {lot_size} │ Max trades: {max_trades}")
        strategy.describe()
        return runner
    call_key, call_name = inst_key, inst_name
    put_key, put_name = None, None
    if "CE" in inst_name.upper():
        call_key, call_name = inst_key, inst_name
        print(f"\n  You picked CE. Finding the matching PE...")
        put_key, put_name = _find_counterpart_option(inst_name, inst_key, sub_keys, sub_meta, client)
    elif "PE" in inst_name.upper():
        put_key, put_name = inst_key, inst_name
        print(f"\n  You picked PE. Finding the matching CE...")
        call_key, call_name = _find_counterpart_option(inst_name, inst_key, sub_keys, sub_meta, client)
    else:
        print(f"  ⚠ Can't determine CE/PE from '{inst_name}'. Using as CALL side.")
    if not call_key or not put_key:
        print(f"  ⚠ Could not find both CE and PE. Falling back to single-side mode.")
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
        strategy.future_key = future_key
        strategy.require_lower_wick = temp.require_lower_wick
        strategy.block_marubozu = temp.block_marubozu
        strategy.max_range = temp.max_range
        strategy.max_momentum = temp.max_momentum
        strategy.max_above_high = temp.max_above_high
        strategy.rebuy_drop = temp.rebuy_drop
        strategy.max_hold = temp.max_hold
        strategy.target_profit = temp.target_profit
        strategy.first_only = temp.first_only
        trader = PaperTrader(inst_name, lot_size=lot_size, max_trades=max_trades)
        return StrategyRunner(strategy, trader, inst_key, future_key=future_key)
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
    call_strat.future_key = future_key
    call_strat.require_lower_wick = temp.require_lower_wick
    call_strat.block_marubozu = temp.block_marubozu
    call_strat.max_range = temp.max_range
    call_strat.max_momentum = temp.max_momentum
    call_strat.max_above_high = temp.max_above_high
    call_strat.rebuy_drop = temp.rebuy_drop
    call_strat.max_hold = temp.max_hold
    call_strat.target_profit = temp.target_profit
    call_strat.first_only = temp.first_only
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
    put_strat.future_key = future_key
    put_strat.require_lower_wick = temp.require_lower_wick
    put_strat.block_marubozu = temp.block_marubozu
    put_strat.max_range = temp.max_range
    put_strat.max_momentum = temp.max_momentum
    put_strat.max_above_high = temp.max_above_high
    put_strat.rebuy_drop = temp.rebuy_drop
    put_strat.max_hold = temp.max_hold
    put_strat.target_profit = temp.target_profit
    put_strat.first_only = temp.first_only
    call_trader = PaperTrader(call_name, lot_size=lot_size, max_trades=max_trades)
    put_trader = PaperTrader(put_name, lot_size=lot_size, max_trades=max_trades)
    runner = DualStrategyRunner(call_strat, call_trader, call_key, put_strat, put_trader, put_key, future_key)
    fut_display = sub_meta.get(future_key, {}).get("name", future_key) if future_key else "none"
    print(f"\n  ✓ Dual CE+PE Momentum")
    print(f"    CE: {call_name:<28} {call_key}")
    print(f"    PE: {put_name:<28} {put_key}")
    print(f"    Future: {fut_display}")
    print(f"    Lot: {lot_size} │ Max trades per side: {max_trades}")
    print(f"\n  ── CALL side ──")
    call_strat.describe()
    print(f"  ── PUT side ──")
    put_strat.describe()
    return runner
def main():
    global ACCESS_TOKEN, MODE
    log_path = start_logging()
    print("\n" + "=" * 55)
    print("  UPSTOX LIVE DATA COLLECTOR")
    print("  Stream • Save • Analyze • Strategy")
    print("=" * 55)
    print(f"  Log: {log_path}")
    if not ACCESS_TOKEN:
        ACCESS_TOKEN = input("\n  Paste your analytics token: ").strip()
    if not ACCESS_TOKEN:
        print("  No token. Exiting.")
        return
    client = make_client(ACCESS_TOKEN)
    result = show_account_info(ACCESS_TOKEN)
    if result == "downgrade": MODE = "full"
    print("\n  Validating token...")
    validate_token(client)
    sub_keys = []
    sub_meta = {}
    strategy_runner = None
    while True:
        n = len(sub_keys)
        strat_status = f" │ Strategy: {strategy_runner.strategy.name}" if strategy_runner else ""
        print(f"\n  ── Subscription List ({n} instruments, mode: {MODE}{strat_status}) ──")
        if sub_keys:
            for i, k in enumerate(sub_keys, 1):
                meta = sub_meta.get(k, {})
                name = meta.get("name", k.split("|")[-1])
                ltp = meta.get("ltp")
                ltp_str = f"₹{ltp}" if ltp else ""
                strat_mark = " ◀ STRATEGY" if strategy_runner and k == strategy_runner.instrument_key else ""
                print(f"    {i}. {name:<28} {ltp_str:>10}   {k}{strat_mark}")
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
        print(f"    ──────────────────")
        print(f"    0. START STREAMING")
        print(f"    q. Quit")
        choice = input(f"\n  > ").strip()
        if choice == "1": add_index_options(client, sub_keys, sub_meta)
        elif choice == "2": add_index_for_streaming(client, sub_keys, sub_meta)
        elif choice == "3": add_futures(client, sub_keys, sub_meta)
        elif choice == "4": add_stock(client, sub_keys, sub_meta)
        elif choice == "5": add_custom_key(client, sub_keys, sub_meta)
        elif choice == "6": add_mcx_commodity(client, sub_keys, sub_meta)
        elif choice == "7": add_currency(client, sub_keys, sub_meta)
        elif choice == "8":
            if not sub_keys:
                print("  Nothing to remove.")
                continue
            pick = input("  Remove # (or 'clear'): ").strip()
            if pick.lower() == "clear":
                sub_keys.clear(); sub_meta.clear(); strategy_runner = None
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
                except (ValueError, IndexError): print("  Invalid.")
        elif choice == "9":
            print(f"  Modes: full, full_d30, ltpc, option_greeks")
            new_mode = input(f"  New mode [{MODE}]: ").strip() or MODE
            if new_mode in ("full", "full_d30", "ltpc", "option_greeks"):
                MODE = new_mode
                print(f"  ✓ Mode set to '{MODE}'")
            else: print(f"  Invalid mode.")
        elif choice == "10":
            if not sub_keys:
                print("  Add instruments first, then set strategy.")
                continue
            strategy_runner = setup_strategy(sub_keys, sub_meta, client=client)
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
            stream(client, sub_keys, MODE, sub_meta, strategy_runner=strategy_runner)
            break
        elif choice.lower() == "q":
            print("  Bye!")
            return
        else: print("  Invalid choice.")
if __name__ == "__main__":
    try:
        main()
    finally:
        stop_logging()
