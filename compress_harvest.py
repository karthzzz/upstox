#!/usr/bin/env python3
"""
compress_harvest.py — Compress JSONL harvest data to Parquet + zstd.
Lossless: every field, every character preserved. Reversible.

Usage:
  python compress_harvest.py harvest_data/               # compress all days
  python compress_harvest.py harvest_data/2026-09-25     # compress one day
  python compress_harvest.py harvest_data/ --verify       # compress + verify round-trip

Output goes to: harvest_compressed_YYYYMMDD_HHMMSS/
  Same folder structure as original. Originals never touched.

Compression: ~10GB/day JSONL → ~200-400MB/day Parquet (20-30x ratio)
"""

import os, sys, glob, json, time
from datetime import datetime
from pathlib import Path


def compress_jsonl(jsonl_path, parquet_path):
    """Compress one JSONL file to Parquet with zstd. Lossless."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("  pip install pandas pyarrow")
        sys.exit(1)

    rows = []
    with open(jsonl_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(line)  # store raw JSON string — LOSSLESS

    if not rows:
        return 0, 0

    original_size = os.path.getsize(jsonl_path)

    # Extract timestamps for indexing (not for data — raw string IS the data)
    timestamps = []
    for r in rows:
        try:
            ts = json.loads(r).get("ts", "")
        except:
            ts = ""
        timestamps.append(ts)

    table = pa.table({
        "ts": pa.array(timestamps, type=pa.string()),
        "raw": pa.array(rows, type=pa.string()),
    })

    # Create parent dirs if needed
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    pq.write_table(table, parquet_path, compression='zstd', compression_level=10)

    compressed_size = os.path.getsize(parquet_path)
    return original_size, compressed_size


def decompress_parquet(parquet_path, jsonl_path):
    """Decompress Parquet back to JSONL. For verification."""
    import pyarrow.parquet as pq
    table = pq.read_table(parquet_path, columns=["raw"])
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)
    with open(jsonl_path, 'w') as f:
        for row in table.column("raw").to_pylist():
            f.write(row + "\n")


def verify_roundtrip(jsonl_path, parquet_path):
    """Verify compression is lossless."""
    import tempfile
    tmp = tempfile.mktemp(suffix=".jsonl")
    decompress_parquet(parquet_path, tmp)

    with open(jsonl_path, newline='') as f1, open(tmp, newline='') as f2:
        orig_lines = [l.rstrip('\r\n') for l in f1 if l.rstrip('\r\n')]
        decomp_lines = [l.rstrip('\r\n') for l in f2 if l.rstrip('\r\n')]

    os.remove(tmp)

    if len(orig_lines) != len(decomp_lines):
        return False, f"Line count: {len(orig_lines)} vs {len(decomp_lines)}"

    for i, (a, b) in enumerate(zip(orig_lines, decomp_lines)):
        if a != b:
            return False, f"Line {i+1} differs"

    return True, f"perfect match ({len(orig_lines)} lines)"


def compress_day(day_dir, output_day_dir, verify=False):
    """Compress all JSONL files in a day directory to output_day_dir."""
    jsonl_files = sorted(glob.glob(os.path.join(day_dir, "*.jsonl")))
    jsonl_files += sorted(glob.glob(os.path.join(day_dir, "*", "*.jsonl")))
    jsonl_files = [f for f in jsonl_files if not os.path.basename(f).startswith("_")]

    if not jsonl_files:
        return 0, 0

    total_orig = 0
    total_comp = 0

    print(f"  {os.path.basename(day_dir)}: {len(jsonl_files)} files → {output_day_dir}", flush=True)

    for jf in jsonl_files:
        # Mirror the relative path: harvest_data/2026-09-25/BANKNIFTY/file.jsonl
        # → output_dir/2026-09-25/BANKNIFTY/file.parquet
        rel = os.path.relpath(jf, day_dir)
        pf = os.path.join(output_day_dir, rel.replace(".jsonl", ".parquet"))

        if os.path.exists(pf):
            # Verify it's a valid parquet (not a partial/corrupt write)
            try:
                import pyarrow.parquet as pq
                meta = pq.read_metadata(pf)
                if meta.num_rows > 0:
                    total_comp += os.path.getsize(pf)
                    continue
                else:
                    os.remove(pf)  # empty parquet — redo
            except Exception:
                os.remove(pf)  # corrupt parquet — redo
                print(f"    ⚠ Removed corrupt {os.path.basename(pf)}, recompressing...")

        orig, comp = compress_jsonl(jf, pf)
        total_orig += orig
        total_comp += comp

        ratio = orig / max(comp, 1)
        name = rel
        print(f"    {name}: {orig/1e6:.1f}MB → {comp/1e6:.1f}MB ({ratio:.0f}x)", flush=True)

        if verify:
            ok, msg = verify_roundtrip(jf, pf)
            if ok:
                print(f"      Verify: ✓ {msg}")
            else:
                print(f"      ✗ VERIFICATION FAILED: {msg}")
                print(f"      Removing bad parquet file")
                os.remove(pf)
                total_comp -= comp

    # Copy metadata files (non-JSONL) to output
    meta_files = glob.glob(os.path.join(day_dir, "_*.json"))
    meta_files += glob.glob(os.path.join(day_dir, "_*.jsonl"))
    meta_files += glob.glob(os.path.join(day_dir, "*", "_*.json"))
    meta_files += glob.glob(os.path.join(day_dir, "*", "_*.jsonl"))
    for mf in meta_files:
        rel = os.path.relpath(mf, day_dir)
        dest = os.path.join(output_day_dir, rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if not os.path.exists(dest):
            import shutil
            shutil.copy2(mf, dest)

    return total_orig, total_comp


def main():
    if len(sys.argv) < 2:
        print("Usage: python compress_harvest.py <harvest_data_path> [--verify]")
        print()
        print("Examples:")
        print("  python compress_harvest.py harvest_data/              # all days")
        print("  python compress_harvest.py harvest_data/2026-09-25    # one day")
        print("  python compress_harvest.py harvest_data/ --verify     # with verification")
        print()
        print("Output: harvest_compressed_YYYYMMDD_HHMMSS/ (same structure, originals untouched)")
        sys.exit(1)

    path = sys.argv[1].rstrip("/\\")
    verify = "--verify" in sys.argv

    # Create output folder with timestamp
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = os.path.basename(path)

    start = time.time()
    grand_orig = 0
    grand_comp = 0

    print(f"Starting compression...")
    print(f"  Source: {os.path.abspath(path)}")
    print(f"  Verify: {verify}")
    print(flush=True)

    if os.path.isdir(path):
        # Check if it's root dir (has date-named subdirs like 2026-08-14/)
        # or a single day dir (has JSONL files directly or in index subdirs)
        date_subdirs = [d for d in os.listdir(path)
                       if os.path.isdir(os.path.join(path, d)) and d[:2] == "20" and len(d) == 10]
        is_root = len(date_subdirs) > 0

        if not is_root:
            # Single day directory
            parent = os.path.dirname(path)
            output_root = os.path.join(parent or ".", f"harvest_compressed_{ts}")
            output_day = os.path.join(output_root, os.path.basename(path))
            orig, comp = compress_day(path, output_day, verify=verify)
            grand_orig += orig
            grand_comp += comp
        else:
            # Root directory — process each day
            output_root = os.path.join(os.path.dirname(path) or ".", f"{base_name}_compressed_{ts}")
            days = sorted(date_subdirs)
            print(f"Found {len(days)} days", flush=True)
            print(f"Output: {output_root}/", flush=True)
            print()

            for d in days:
                orig, comp = compress_day(
                    os.path.join(path, d),
                    os.path.join(output_root, d),
                    verify=verify
                )
                grand_orig += orig
                grand_comp += comp

            if grand_orig > 0:
                print(f"\n{'═'*60}")
                print(f"  TOTAL: {grand_orig/1e9:.1f}GB → {grand_comp/1e9:.2f}GB ({grand_orig/max(grand_comp,1):.0f}x)")
                print(f"  Saved: {(grand_orig-grand_comp)/1e9:.1f}GB")
    else:
        print(f"Path not found: {path}")
        sys.exit(1)

    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.0f}s")
    print(f"Output at: {output_root}")
    print(f"Originals untouched at: {path}")


if __name__ == "__main__":
    main()
