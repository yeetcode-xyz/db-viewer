"""
Fix BLOB values stored in INTEGER columns.

Some cells in the DB hold raw 8-byte (or 4-byte / 1-byte) little-endian
integer blobs where a real integer was expected. This script finds those
cells and rewrites them with the decoded integer value.

Usage:
    # Dry run — shows what would change, writes nothing:
    python cleanup_blob_ints.py --db path/to/yeetcode.db

    # Actually apply the fixes (also writes a .bak backup first):
    python cleanup_blob_ints.py --db path/to/yeetcode.db --apply
"""
import argparse
import shutil
import sqlite3
import struct
import sys
from collections import Counter


def is_int_type(decl_type):
    """True if a column's declared type is integer-ish (INTEGER, INT, BOOL, ...)."""
    if not decl_type:
        return False
    d = decl_type.upper()
    return "INT" in d or "BOOL" in d


def decode_int_blob(raw):
    """Decode little-endian integer bytes. Returns int, or None if we don't recognise the size."""
    if not isinstance(raw, (bytes, bytearray, memoryview)):
        return None
    b = bytes(raw)
    if len(b) == 8:
        return struct.unpack("<q", b)[0]   # signed int64
    if len(b) == 4:
        return struct.unpack("<i", b)[0]   # signed int32
    if len(b) == 1:
        return b[0]                        # single byte
    return None


def scan(conn):
    """Return list of (table, column, rowid, raw_bytes, decoded_or_None) for every
    BLOB cell found in an INTEGER-declared column."""
    cur = conn.cursor()
    tables = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )]
    findings = []
    for t in tables:
        cols = cur.execute(f'PRAGMA table_info("{t}")').fetchall()
        # PRAGMA returns: (cid, name, type, notnull, dflt_value, pk)
        int_cols = [c[1] for c in cols if is_int_type(c[2])]
        for col in int_cols:
            rows = cur.execute(
                f'SELECT rowid, "{col}" FROM "{t}" WHERE typeof("{col}") = \'blob\''
            ).fetchall()
            for rowid, raw in rows:
                findings.append((t, col, rowid, raw, decode_int_blob(raw)))
    return findings


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True, help="Path to sqlite DB file")
    ap.add_argument("--apply", action="store_true",
                    help="Actually write changes (default is dry run)")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        findings = scan(conn)
        if not findings:
            print("No BLOB-in-INTEGER-column cells found. Nothing to do.")
            return 0

        decodable   = [f for f in findings if f[4] is not None]
        undecodable = [f for f in findings if f[4] is None]

        print(f"Found {len(findings)} cell(s) with BLOB values in INTEGER columns.")
        print(f"  Decodable (will be fixed): {len(decodable)}")
        print(f"  Undecodable (will be skipped): {len(undecodable)}")
        print()

        # Count hits per (table, column)
        summary = Counter((t, c) for t, c, *_ in findings)
        print("Affected columns:")
        for (t, c), n in summary.most_common():
            print(f"  {t}.{c}: {n} cell(s)")
        print()

        print("Sample (up to 10):")
        for t, c, rowid, raw, decoded in decodable[:10]:
            raw_hex = bytes(raw).hex()
            print(f"  {t}[rowid={rowid}].{c}: 0x{raw_hex}  ->  {decoded}")
        print()

        if undecodable:
            print(f"WARNING: {len(undecodable)} cell(s) have unexpected byte lengths and will be SKIPPED:")
            for t, c, rowid, raw, _ in undecodable[:10]:
                print(f"  {t}[rowid={rowid}].{c}: {len(bytes(raw))} bytes")
            print()

        if not args.apply:
            print("Dry run — no changes written. Re-run with --apply to commit.")
            return 0

        # Backup before writing
        backup = args.db + ".bak"
        shutil.copy2(args.db, backup)
        print(f"Backup written to: {backup}")

        cur = conn.cursor()
        try:
            for t, c, rowid, _, decoded in decodable:
                cur.execute(
                    f'UPDATE "{t}" SET "{c}" = ? WHERE rowid = ?',
                    (decoded, rowid),
                )
            conn.commit()
            print(f"OK: updated {len(decodable)} cell(s). Skipped {len(undecodable)}.")
        except Exception as e:
            conn.rollback()
            print(f"ERROR during update — rolled back, DB unchanged. {e}", file=sys.stderr)
            return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
