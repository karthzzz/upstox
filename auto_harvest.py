"""
Upstox Auto Data Harvester — Local PC Version
================================================
Runs automatically every market day. No manual intervention needed.

What it does:
  1. Auto-discovers nearest 3 expiries for BANKNIFTY, NIFTY, SENSEX
  2. Fetches option chains, selects ATM ± 25 strikes (CE + PE)
  3. Finds nearest 3 futures per index
  4. Subscribes to all (~900-1000 instruments) via WebSocket
  5. Streams full tick data, saves to JSONL files (one per instrument)
  6. Organized by index: harvest_data/YYYY-MM-DD/BANKNIFTY/, NIFTY/, SENSEX/
  7. Auto-stops after market close (3:31 PM IST)

Output structure:
    harvest_data/
      2026-08-14/
        BANKNIFTY/
          BANKNIFTY_INDEX__NSE_INDEX_Nifty_Bank.jsonl
          BANKNIFTY_FUT_2026-08-25__NSE_FO_58067.jsonl
          BANKNIFTY_57500CE_2026-08-25__NSE_FO_59090.jsonl
          BANKNIFTY_57500PE_2026-08-25__NSE_FO_59091.jsonl
          ...
        NIFTY/
          NIFTY_INDEX__NSE_INDEX_Nifty_50.jsonl
          ...
        SENSEX/
          SENSEX_INDEX__BSE_INDEX_SENSEX.jsonl
          ...
        _market_info.jsonl
        _metadata.json

Install:
    pip install upstox-python-sdk

Run once:     python auto_harvest.py
Force run:    python auto_harvest.py --force
Daemon:       python auto_harvest.py --daemon
With token:   python auto_harvest.py --token "eyJ..."

Cron (recommended):
    10 9 * * 1-5 cd /path/to/project && python auto_harvest.py >> logs/harvest.log 2>&1
"""

import json
import os
import sys
import time
import logging
import argparse
import threading
from datetime import datetime, date, timedelta, timezone

import upstox_client
from upstox_client.rest import ApiException

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

ACCESS_TOKEN = ""  # paste here, or --token, or UPSTOX_TOKEN env var
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI4MkJOVUUiLCJqdGkiOiI2YTdkZDE2NWE5MTc1NjI3NTA2MjBhZWUiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaXNFeHRlbmRlZCI6dHJ1ZSwiaWF0IjoxNzg2NjMwNTAxLCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE4MTgxOTQ0MDB9.xyCsppJkRZAjRzpPNuav6wHFt975GsK2gc2VRdrl6IA"

# Streaming mode: "full" = LTPC + 5-level depth + OHLC + greeks + OI + volume (max 2000 instruments)
MODE = "full"

# ATM ± 25 = ~50 strikes × 2 sides = ~100 options per expiry
STRIKES_AROUND_ATM = 25

# Indices to harvest
HARVEST_INDICES = {
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "NIFTY":     "NSE_INDEX|Nifty 50",
    "SENSEX":    "BSE_INDEX|SENSEX",
}

# How many nearest expiries per index
NUM_EXPIRIES = 3

# Max futures per index (near, next, far)
MAX_FUTURES = 3

# Market hours (IST)
MARKET_START_HOUR, MARKET_START_MIN = 9, 14
MARKET_END_HOUR, MARKET_END_MIN = 15, 31

# Output directory
SAVE_DIR = "harvest_data"
LOG_DIR = "logs"

# Delay between API calls during discovery (avoid rate limits)
API_DELAY = 0.3

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"harvest_{date.today().isoformat()}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
log = logging.getLogger("harvester")


# ═══════════════════════════════════════════════════════════════
# TIME HELPERS (all IST-aware)
# ═══════════════════════════════════════════════════════════════

def now_ist():
    return datetime.now(IST)

def is_market_day():
    return now_ist().weekday() < 5

def is_market_hours():
    now = now_ist()
    start = now.replace(hour=MARKET_START_HOUR, minute=MARKET_START_MIN, second=0, microsecond=0)
    end = now.replace(hour=MARKET_END_HOUR, minute=MARKET_END_MIN, second=0, microsecond=0)
    return start <= now <= end

def seconds_until_market_start():
    now = now_ist()
    start = now.replace(hour=MARKET_START_HOUR, minute=MARKET_START_MIN, second=0, microsecond=0)
    return max(0, (start - now).total_seconds())

def seconds_until_market_end():
    now = now_ist()
    end = now.replace(hour=MARKET_END_HOUR, minute=MARKET_END_MIN, second=0, microsecond=0)
    return max(0, (end - now).total_seconds())


# ═══════════════════════════════════════════════════════════════
# SDK HELPERS
# ═══════════════════════════════════════════════════════════════

def make_client(token):
    config = upstox_client.Configuration()
    config.access_token = token
    return upstox_client.ApiClient(config)


def clean_expiry(val):
    s = str(val).strip()
    if " " in s:
        s = s.split(" ")[0]
    if "T" in s:
        s = s.split("T")[0]
    return s


def validate_token(client):
    try:
        api = upstox_client.MarketQuoteV3Api(client)
        resp = api.get_ltp(instrument_key="NSE_INDEX|Nifty 50")
        data = resp.data if hasattr(resp, "data") else {}
        if data:
            for key, val in (data.items() if isinstance(data, dict) else []):
                d = val.to_dict() if hasattr(val, "to_dict") else val
                ltp = d.get("last_price") or d.get("ltp")
                if ltp:
                    return ltp
        return True
    except Exception as e:
        raise RuntimeError(f"Token validation failed: {e}")


# ═══════════════════════════════════════════════════════════════
# INSTRUMENT DISCOVERY
# ═══════════════════════════════════════════════════════════════

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
        log.error(f"Failed to get expiries for {underlying_key}: {e}")
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
            chain.append({
                "strike":   e.get("strike_price"),
                "spot":     e.get("underlying_spot_price"),
                "call_key": call.get("instrument_key"),
                "call_oi":  cm.get("oi", 0),
                "put_key":  put.get("instrument_key"),
                "put_oi":   pm.get("oi", 0),
            })
        chain.sort(key=lambda x: x["strike"] or 0)
        return chain
    except ApiException as e:
        log.error(f"Failed to get chain for {underlying_key} exp={expiry}: {e}")
        return []


def search_futures(client, index_name, index_key):
    """Find futures for an index. Filters out variants (SENSEX50, BANKNIFTY from NIFTY search)."""
    api = upstox_client.InstrumentsApi(client)
    try:
        resp = api.search_instrument(index_name)
        data = resp.data if hasattr(resp, "data") else []
        exchange = index_key.split("_")[0]  # "NSE" or "BSE"
        futures = []
        for r in data:
            d = r.to_dict() if hasattr(r, "to_dict") else r
            itype = (d.get("instrument_type") or "").upper()
            ikey = d.get("instrument_key") or ""
            iname = (d.get("name") or d.get("trading_symbol") or "").upper()
            if "FUT" not in itype:
                continue
            if not ikey.startswith(f"{exchange}_FO"):
                continue
            if index_name == "SENSEX" and "SENSEX50" in iname:
                continue
            if index_name == "NIFTY" and "BANK" in iname:
                continue
            expiry = clean_expiry(d.get("expiry") or "")
            futures.append({"key": ikey, "expiry": expiry})
        # Sort by expiry so we get near, next, far month
        futures.sort(key=lambda x: x["expiry"])
        return futures[:MAX_FUTURES]
    except Exception as e:
        log.warning(f"Futures search failed for {index_name}: {e}")
        return []


def discover_instruments(client):
    """
    Auto-discover all instruments.
    Returns (keys_list, meta_dict, key_to_index_dict).

    meta:         instrument_key → readable description (e.g. "BANKNIFTY 57500CE 2026-08-25")
    key_to_index: instrument_key → index name ("BANKNIFTY" / "NIFTY" / "SENSEX")
    """
    all_keys = []
    meta = {}           # key → description string for filenames
    key_to_index = {}   # key → which index folder it belongs to

    for name, index_key in HARVEST_INDICES.items():
        log.info(f"── {name} ──")

        # 1. Index itself
        all_keys.append(index_key)
        meta[index_key] = f"{name} INDEX"
        key_to_index[index_key] = name
        log.info(f"  Index: {index_key}")

        # 2. Futures
        time.sleep(API_DELAY)
        futures = search_futures(client, name, index_key)
        for fut in futures:
            fk = fut["key"]
            fexp = fut["expiry"]
            all_keys.append(fk)
            meta[fk] = f"{name} FUT {fexp}" if fexp else f"{name} FUT"
            key_to_index[fk] = name
        log.info(f"  Futures: {len(futures)}")

        # 3. Expiries
        time.sleep(API_DELAY)
        expiries = get_expiries(client, index_key)
        if not expiries:
            log.warning(f"  No expiries found for {name}")
            continue

        selected_expiries = expiries[:NUM_EXPIRIES]
        log.info(f"  Expiries: {selected_expiries}")

        # 4. Option chains per expiry
        for expiry in selected_expiries:
            time.sleep(API_DELAY)
            chain = get_option_chain(client, index_key, expiry)
            if not chain:
                log.warning(f"  No chain for {name} {expiry}")
                continue

            spot = chain[0].get("spot", 0) if chain else 0
            if not spot:
                spot = max(chain, key=lambda s: (s.get("call_oi", 0) + s.get("put_oi", 0))).get("strike", 0)

            atm_idx = min(range(len(chain)), key=lambda i: abs((chain[i]["strike"] or 0) - spot))
            lo = max(0, atm_idx - STRIKES_AROUND_ATM)
            hi = min(len(chain), atm_idx + STRIKES_AROUND_ATM + 1)
            selected = chain[lo:hi]

            count = 0
            for s in selected:
                if s.get("call_key"):
                    k = s["call_key"]
                    all_keys.append(k)
                    meta[k] = f"{name} {int(s['strike'])}CE {expiry}"
                    key_to_index[k] = name
                    count += 1
                if s.get("put_key"):
                    k = s["put_key"]
                    all_keys.append(k)
                    meta[k] = f"{name} {int(s['strike'])}PE {expiry}"
                    key_to_index[k] = name
                    count += 1

            log.info(f"  {expiry}: spot={spot:.0f}, ATM±{STRIKES_AROUND_ATM} → {count} options")

    # Deduplicate preserving order
    seen = set()
    unique = [k for k in all_keys if not (k in seen or seen.add(k))]

    # Summary
    per_index = {}
    for k in unique:
        idx = key_to_index.get(k, "OTHER")
        per_index[idx] = per_index.get(idx, 0) + 1

    log.info(f"\n{'─'*50}")
    log.info(f"  DISCOVERY COMPLETE")
    for idx_name, count in per_index.items():
        log.info(f"    {idx_name}: {count} instruments")
    log.info(f"    TOTAL: {len(unique)} instruments")
    if len(unique) > 2000:
        log.warning(f"  ⚠ Exceeds 2000 limit for 'full' mode! Truncating.")
        unique = unique[:2000]
    log.info(f"{'─'*50}")

    return unique, meta, key_to_index


# ═══════════════════════════════════════════════════════════════
# FILE WRITER — per-instrument files in index subfolders
# ═══════════════════════════════════════════════════════════════

class HarvestWriter:
    """
    Writes each tick to a per-instrument JSONL file, organized by index subfolder.

    Structure:
        base_dir/BANKNIFTY/BANKNIFTY_57500CE_2026-08-25__NSE_FO_59090.jsonl
        base_dir/NIFTY/NIFTY_INDEX__NSE_INDEX_Nifty_50.jsonl
        base_dir/_market_info.jsonl   (root level)
    """

    def __init__(self, save_dir, meta, key_to_index):
        today = date.today().isoformat()
        self.base_dir = os.path.join(save_dir, today)
        self._meta = meta
        self._key_to_index = key_to_index
        self._files = {}
        self._counts = {}
        self._index_counts = {}   # per-index tick counts
        self._lock = threading.Lock()
        self._total = 0
        self._start_time = time.time()

        # Create base + index subdirectories
        os.makedirs(self.base_dir, exist_ok=True)
        for index_name in set(key_to_index.values()):
            os.makedirs(os.path.join(self.base_dir, index_name), exist_ok=True)

    def _build_path(self, key):
        """Build file path: base_dir/INDEX_NAME/readable_name__instrument_key.jsonl"""
        index_name = self._key_to_index.get(key)
        desc = self._meta.get(key, "")

        # Sanitize for filename
        desc_safe = desc.replace(" ", "_").replace("|", "_").replace("/", "-") if desc else ""
        key_safe = key.replace("|", "_").replace(" ", "_")

        if desc_safe:
            fname = f"{desc_safe}__{key_safe}.jsonl"
        else:
            fname = f"{key_safe}.jsonl"

        if index_name:
            return os.path.join(self.base_dir, index_name, fname)
        else:
            # Unknown keys (_market_info, _other, unexpected) go in root
            return os.path.join(self.base_dir, fname)

    def _get_file(self, key):
        if key not in self._files:
            path = self._build_path(key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self._files[key] = open(path, "a", buffering=1)
            self._counts[key] = 0
        return self._files[key]

    def write(self, key, data, ts):
        record = {"ts": ts, "key": key, "data": data}
        with self._lock:
            f = self._get_file(key)
            f.write(json.dumps(record, default=str) + "\n")
            self._counts[key] = self._counts.get(key, 0) + 1
            self._total += 1
            # Track per-index counts
            idx = self._key_to_index.get(key, "_other")
            self._index_counts[idx] = self._index_counts.get(idx, 0) + 1

    def close(self):
        with self._lock:
            for f in self._files.values():
                f.close()
            self._files.clear()

    @property
    def total(self):
        with self._lock:
            return self._total

    @property
    def file_count(self):
        with self._lock:
            return len(self._counts)

    def get_index_counts(self):
        with self._lock:
            return dict(self._index_counts)

    def get_counts(self):
        with self._lock:
            return dict(self._counts)

    @property
    def elapsed_minutes(self):
        return (time.time() - self._start_time) / 60

    @property
    def dir(self):
        return self.base_dir


# ═══════════════════════════════════════════════════════════════
# STREAMING — captures every tick, saves raw data
# ═══════════════════════════════════════════════════════════════

def run_stream(client, instrument_keys, meta, key_to_index, save_dir, force_run=False):
    """Stream market data until market close. Blocks the calling thread."""
    writer = HarvestWriter(save_dir, meta, key_to_index)

    log.info(f"Starting stream: {len(instrument_keys)} instruments in '{MODE}' mode")
    log.info(f"Saving to: {writer.dir}/")
    for idx_name in sorted(set(key_to_index.values())):
        count = sum(1 for v in key_to_index.values() if v == idx_name)
        log.info(f"  {idx_name}/: {count} instruments")

    streamer = upstox_client.MarketDataStreamerV3(client, instrument_keys, MODE)

    last_log = [time.time()]
    last_status = [time.time()]

    def on_open():
        log.info("✓ WebSocket connected. Receiving data...")

    def on_message(message):
        ts = now_ist().isoformat()

        # Parse message — keep raw data exactly as received
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

        # Market info → root level file
        if msg_type == "market_info":
            log.info("Market info received")
            writer.write("_market_info", data, ts)
            return

        # Feed data → one write per instrument per tick
        if feeds:
            for key, feed_data in feeds.items():
                writer.write(key, feed_data, ts)
        else:
            writer.write("_other", data, ts)
            return

        # Progress log every 30 seconds with per-index breakdown
        if time.time() - last_log[0] >= 30:
            last_log[0] = time.time()
            elapsed = max(writer.elapsed_minutes, 0.1)
            tps = writer.total / elapsed
            idx_counts = writer.get_index_counts()
            idx_str = " │ ".join(f"{k}: {v:,}" for k, v in sorted(idx_counts.items()) if k != "_other")
            log.info(
                f"  ticks: {writer.total:,} │ ~{tps:,.0f}/min │ {idx_str} │ "
                f"{now_ist().strftime('%H:%M:%S')} IST"
            )

        # Check market hours every 60 seconds
        if not force_run and time.time() - last_status[0] >= 60:
            last_status[0] = time.time()
            if not is_market_hours():
                log.info("Market hours ended. Stopping stream...")
                try:
                    streamer.disconnect()
                except Exception:
                    pass

    def on_error(error):
        log.error(f"WebSocket error: {error}")

    def on_close(*args):
        log.info("WebSocket closed.")

    streamer.on("open", on_open)
    streamer.on("message", on_message)
    streamer.on("error", on_error)
    streamer.on("close", on_close)
    streamer.auto_reconnect(True, 5, 50)

    # Safety timer
    if not force_run:
        remaining = seconds_until_market_end()
        if remaining > 0:
            log.info(f"Will auto-stop in {remaining/60:.0f} minutes (at {MARKET_END_HOUR}:{MARKET_END_MIN:02d} IST)")
            def force_stop():
                log.info("Market close timer fired. Disconnecting...")
                try:
                    streamer.disconnect()
                except Exception:
                    pass
            timer = threading.Timer(remaining + 60, force_stop)
            timer.daemon = True
            timer.start()

    # Main streaming loop
    try:
        streamer.connect()
        while True:
            time.sleep(1)
            if not force_run and not is_market_hours():
                log.info("Market hours ended. Stopping...")
                break
    except KeyboardInterrupt:
        log.info("Manual interrupt (Ctrl+C).")
    finally:
        try:
            streamer.disconnect()
        except Exception:
            pass
        writer.close()

        # ── End of day summary ──
        elapsed = writer.elapsed_minutes
        idx_counts = writer.get_index_counts()

        log.info(f"\n{'='*55}")
        log.info(f"  HARVEST COMPLETE — {date.today().isoformat()}")
        log.info(f"  Duration:    {elapsed:.0f} minutes")
        log.info(f"  Total ticks: {writer.total:,}")
        log.info(f"  Files:       {writer.file_count}")
        log.info(f"  Avg rate:    {writer.total / max(elapsed, 0.1):,.0f} ticks/min")
        log.info(f"")
        for idx_name in sorted(idx_counts.keys()):
            if idx_name.startswith("_"):
                continue
            count = idx_counts[idx_name]
            folder = os.path.join(writer.dir, idx_name)
            n_files = len([f for f in os.listdir(folder) if f.endswith('.jsonl')]) if os.path.isdir(folder) else 0
            log.info(f"  {idx_name:>12}: {count:>12,} ticks │ {n_files:>4} files")
        log.info(f"")
        log.info(f"  Directory: {writer.dir}/")
        log.info(f"{'='*55}")

    # Save metadata
    meta_path = os.path.join(writer.dir, "_metadata.json")
    with open(meta_path, "w") as f:
        json.dump({
            "date": date.today().isoformat(),
            "mode": MODE,
            "strikes_around_atm": STRIKES_AROUND_ATM,
            "num_expiries": NUM_EXPIRIES,
            "total_instruments": len(instrument_keys),
            "total_ticks": writer.total,
            "duration_minutes": round(elapsed, 1),
            "ticks_per_index": idx_counts,
            "instruments": meta,
            "key_to_index": key_to_index,
            "instrument_keys": instrument_keys,
        }, f, indent=2, default=str)
    log.info(f"Metadata saved: {meta_path}")

    return writer


# ═══════════════════════════════════════════════════════════════
# MAIN ENTRY POINTS
# ═══════════════════════════════════════════════════════════════

def run_harvest(force_run=False):
    global ACCESS_TOKEN

    if not ACCESS_TOKEN:
        ACCESS_TOKEN = os.environ.get("UPSTOX_TOKEN", "")
    if not ACCESS_TOKEN:
        log.error("No access token! Set ACCESS_TOKEN in script, --token flag, or UPSTOX_TOKEN env var.")
        sys.exit(1)

    log.info(f"\n{'='*55}")
    log.info(f"  UPSTOX AUTO HARVESTER")
    log.info(f"  Date: {date.today().isoformat()} │ Mode: {MODE}")
    log.info(f"  Strikes: ATM±{STRIKES_AROUND_ATM} │ Expiries: {NUM_EXPIRIES}")
    log.info(f"  Save dir: {SAVE_DIR}")
    log.info(f"{'='*55}")

    if not force_run and not is_market_day():
        log.info("Not a market day (weekend). Use --force to override.")
        return

    client = make_client(ACCESS_TOKEN)

    try:
        ltp = validate_token(client)
        if isinstance(ltp, (int, float)):
            log.info(f"✓ Token valid. Nifty 50: ₹{ltp}")
        else:
            log.info("✓ Token valid (API responded)")
    except RuntimeError as e:
        log.error(str(e))
        sys.exit(1)

    # Discover instruments
    log.info("\nDiscovering instruments...")
    instrument_keys, meta, key_to_index = discover_instruments(client)

    if not instrument_keys:
        log.error("No instruments discovered. Exiting.")
        return

    # Wait for market
    if not force_run:
        wait = seconds_until_market_start()
        if wait > 0:
            log.info(f"Waiting {wait/60:.0f} minutes for market open ({MARKET_START_HOUR}:{MARKET_START_MIN:02d} IST)...")
            time.sleep(wait)
        if seconds_until_market_end() <= 0:
            log.info("Market already closed for today. Use --force to override.")
            return

    # Stream
    run_stream(client, instrument_keys, meta, key_to_index, SAVE_DIR, force_run=force_run)


def run_daemon():
    log.info("Daemon mode started. Will run every market day.")
    while True:
        now = now_ist()
        if is_market_day():
            wait = seconds_until_market_start()
            if wait > 60:
                log.info(f"Next harvest in {wait/60:.0f} minutes. Sleeping...")
                time.sleep(min(wait - 30, 3600))
                continue
            elif wait > 0:
                time.sleep(wait)
            if is_market_hours():
                log.info("Market hours — starting harvest.")
                try:
                    run_harvest()
                except Exception as e:
                    log.error(f"Harvest failed: {e}", exc_info=True)
                log.info("Harvest done. Sleeping until tomorrow...")
                tomorrow_9am = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
                sleep_secs = (tomorrow_9am - now_ist()).total_seconds()
                if sleep_secs > 0:
                    time.sleep(sleep_secs)
                continue
        log.info(f"Outside market hours ({now.strftime('%A %H:%M')} IST). Sleeping 1 hour...")
        time.sleep(3600)


def _in_notebook():
    try:
        from IPython import get_ipython
        return get_ipython() is not None
    except Exception:
        return False


if __name__ == "__main__":
    if _in_notebook():
        run_harvest(force_run=True)
    else:
        parser = argparse.ArgumentParser(description="Upstox Auto Data Harvester")
        parser.add_argument("--daemon", action="store_true", help="Run as daemon (daily)")
        parser.add_argument("--force", action="store_true", help="Skip market hour/day checks")
        parser.add_argument("--token", type=str, help="Access token")
        parser.add_argument("--strikes", type=int, default=STRIKES_AROUND_ATM, help="Strikes around ATM")
        parser.add_argument("--expiries", type=int, default=NUM_EXPIRIES, help="Number of expiries")
        parser.add_argument("--mode", choices=["full", "ltpc", "option_greeks"], default=MODE)
        parser.add_argument("--save-dir", type=str, default=SAVE_DIR, help="Output directory")
        args = parser.parse_args()

        if args.token:
            ACCESS_TOKEN = args.token
        STRIKES_AROUND_ATM = args.strikes
        NUM_EXPIRIES = args.expiries
        MODE = args.mode
        SAVE_DIR = args.save_dir

        if args.daemon:
            run_daemon()
        else:
            run_harvest(force_run=args.force)