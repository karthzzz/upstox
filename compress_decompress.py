#!/usr/bin/env python3
"""
HARVEST DATA COMPRESSOR / DECOMPRESSOR
========================================
Compresses JSONL files to a SEPARATE output folder using zstd.
Original files are NEVER modified or deleted.
Byte-for-byte identical when decompressed.

  Compress:   python compress_harvest.py compress /path/to/harvest_data /path/to/compressed
  Decompress: python compress_harvest.py decompress /path/to/compressed /path/to/restored
  Status:     python compress_harvest.py status /path/to/harvest_data /path/to/compressed

153 GB → ~15-25 GB. Exact original restored on decompress.
"""

import os
import sys
import hashlib
import struct
from pathlib import Path

def check_deps():
    try:
        import zstandard
        return True
    except ImportError:
        print("  Installing zstandard...")
        os.system(f"{sys.executable} -m pip install zstandard --break-system-packages -q")
        return True


def compress_file(src_path, dst_path):
    """Compress a single file. Returns (success, original_size, compressed_size)."""
    import zstandard as zstd

    if os.path.exists(dst_path):
        return True, 0, 0  # already done

    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    original_size = os.path.getsize(src_path)

    cctx = zstd.ZstdCompressor(level=10, threads=-1)
    with open(src_path, 'rb') as f_in, open(dst_path, 'wb') as f_out:
        cctx.copy_stream(f_in, f_out)

    compressed_size = os.path.getsize(dst_path)

    # Verify first 4KB matches
    dctx = zstd.ZstdDecompressor()
    with open(dst_path, 'rb') as f_comp:
        reader = dctx.stream_reader(f_comp)
        verify_chunk = reader.read(4096)
    with open(src_path, 'rb') as f_orig:
        original_chunk = f_orig.read(4096)

    if verify_chunk != original_chunk:
        print(f"⚠ VERIFY FAILED")
        os.remove(dst_path)
        return False, original_size, 0

    return True, original_size, compressed_size


def decompress_file(src_path, dst_path):
    """Decompress a .zst file. Returns (success, compressed_size, restored_size)."""
    import zstandard as zstd

    if os.path.exists(dst_path):
        return True, 0, 0  # already exists

    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    compressed_size = os.path.getsize(src_path)

    dctx = zstd.ZstdDecompressor()
    with open(src_path, 'rb') as f_in, open(dst_path, 'wb') as f_out:
        dctx.copy_stream(f_in, f_out)

    restored_size = os.path.getsize(dst_path)
    return True, compressed_size, restored_size


def do_compress(src_dir, dst_dir):
    """Compress all JSONL files from src_dir into dst_dir with same folder structure."""
    src_path = Path(src_dir)
    dst_path = Path(dst_dir)

    if not src_path.exists():
        print(f"  ⚠ Source not found: {src_dir}")
        return

    if str(src_path.resolve()) == str(dst_path.resolve()):
        print(f"  ⚠ Source and destination are the same folder! Use a different output path.")
        return

    jsonl_files = sorted(src_path.rglob("*.jsonl"))
    if not jsonl_files:
        print(f"  No .jsonl files found in {src_dir}")
        return

    total_size = sum(f.stat().st_size for f in jsonl_files)
    print(f"  Source:  {src_dir} ({len(jsonl_files)} files, {total_size / (1024**3):.1f} GB)")
    print(f"  Output:  {dst_dir}")
    print()

    total_saved = 0
    compressed_count = 0
    skipped = 0
    failed = 0

    for i, f in enumerate(jsonl_files, 1):
        rel = f.relative_to(src_path)
        dst_file = dst_path / (str(rel) + ".zst")

        size_mb = f.stat().st_size / (1024*1024)
        print(f"  [{i}/{len(jsonl_files)}] {rel} ({size_mb:.0f} MB)...", end=" ", flush=True)

        success, orig_size, comp_size = compress_file(str(f), str(dst_file))

        if not success:
            failed += 1
            print("FAILED")
        elif comp_size == 0:
            skipped += 1
            print("skip")
        else:
            ratio = orig_size / max(comp_size, 1)
            total_saved += orig_size - comp_size
            compressed_count += 1
            print(f"✓ {comp_size/1024/1024:.0f} MB ({ratio:.1f}x)")

    print(f"\n{'═'*50}")
    print(f"  Compressed: {compressed_count} files")
    print(f"  Skipped: {skipped} (already done)")
    print(f"  Failed: {failed}")
    print(f"  Space saved: {total_saved / (1024**3):.1f} GB")
    print(f"\n  Originals in {src_dir} are UNTOUCHED.")
    print(f"  Compressed copies in {dst_dir}")


def do_decompress(src_dir, dst_dir):
    """Decompress all .jsonl.zst files from src_dir into dst_dir."""
    src_path = Path(src_dir)
    dst_path = Path(dst_dir)

    if not src_path.exists():
        print(f"  ⚠ Source not found: {src_dir}")
        return

    zst_files = sorted(src_path.rglob("*.jsonl.zst"))
    if not zst_files:
        print(f"  No .jsonl.zst files found in {src_dir}")
        return

    total_size = sum(f.stat().st_size for f in zst_files)
    print(f"  Source:  {src_dir} ({len(zst_files)} files, {total_size / (1024**3):.1f} GB)")
    print(f"  Output:  {dst_dir}")
    print()

    total_restored = 0
    count = 0

    for i, f in enumerate(zst_files, 1):
        rel = f.relative_to(src_path)
        # Remove .zst to get original name
        dst_file = dst_path / str(rel)[:-4]  # remove .zst

        print(f"  [{i}/{len(zst_files)}] {rel}...", end=" ", flush=True)

        success, comp_size, restored_size = decompress_file(str(f), str(dst_file))

        if restored_size > 0:
            total_restored += restored_size
            count += 1
            print(f"✓ {restored_size/1024/1024:.0f} MB")
        else:
            print("skip (exists)")

    print(f"\n{'═'*50}")
    print(f"  Restored: {count} files ({total_restored / (1024**3):.1f} GB)")
    print(f"  Output: {dst_dir}")


def do_status(src_dir, dst_dir):
    """Show compression status."""
    src_path = Path(src_dir)
    dst_path = Path(dst_dir)

    jsonl_files = list(src_path.rglob("*.jsonl")) if src_path.exists() else []
    zst_files = list(dst_path.rglob("*.jsonl.zst")) if dst_path.exists() else []

    jsonl_size = sum(f.stat().st_size for f in jsonl_files) if jsonl_files else 0
    zst_size = sum(f.stat().st_size for f in zst_files) if zst_files else 0

    print(f"  Original:   {len(jsonl_files)} files, {jsonl_size / (1024**3):.1f} GB  ({src_dir})")
    print(f"  Compressed: {len(zst_files)} files, {zst_size / (1024**3):.1f} GB  ({dst_dir})")

    if jsonl_size > 0 and zst_size > 0:
        print(f"  Ratio: {jsonl_size / max(zst_size, 1):.1f}x smaller")
        print(f"  Saving: {(jsonl_size - zst_size) / (1024**3):.1f} GB")

    if len(jsonl_files) != len(zst_files):
        print(f"\n  ⚠ Count mismatch: {len(jsonl_files)} originals vs {len(zst_files)} compressed")
        print(f"    Run compress again to finish remaining files.")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage:")
        print(f"  python {sys.argv[0]} compress   /path/to/harvest_data  [output_folder]")
        print(f"  python {sys.argv[0]} decompress /path/to/compressed    [output_folder]")
        print(f"  python {sys.argv[0]} status     /path/to/harvest_data  [compressed_folder]")
        print()
        print("  Output folder is optional — auto-creates harvest_compressed_YYYYMMDD_HHMMSS")
        sys.exit(1)

    check_deps()

    action = sys.argv[1].lower()
    src = sys.argv[2]

    # Auto-generate output folder with datetime if not provided
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if len(sys.argv) > 3 and not sys.argv[3].startswith("--"):
        dst = sys.argv[3]
    elif action == "compress":
        dst = f"harvest_compressed_{timestamp}"
    elif action == "decompress":
        dst = f"harvest_restored_{timestamp}"
    else:
        dst = src  # status just needs src

    if action == "compress":
        do_compress(src, dst)
    elif action == "decompress":
        do_decompress(src, dst)
    elif action == "status":
        do_status(src, dst)
    else:
        print(f"Unknown: {action}. Use compress, decompress, or status.")