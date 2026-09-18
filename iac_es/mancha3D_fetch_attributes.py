#!/usr/bin/env python3
"""
ftp_recover_hdf5_attrs.py

Connects to an FTP server, downloads only the first N bytes of every
file in a given remote directory, and attempts to recover as many
root-group HDF5 attributes as possible from each partial download --
without ever pulling the full file over the network.

This reuses the same trick as recover_hdf5_attrs.py: an HDF5
superblock (at byte 0) records the file's true expected size. Once we
have that value we pad the partial download with zero bytes up to it,
in memory, so h5py's size check passes and we can read whatever
attributes survived in the bytes we actually downloaded.

Usage:
    python ftp_recover_hdf5_attrs.py --directory /some/remote/dir

    python ftp_recover_hdf5_attrs.py \\
        --server lapalma3.iac.es \\
        --user anonymous \\
        --password anonymous \\
        --directory /some/remote/dir \\
        --size 4096 \\
        --pattern "*.h5"
"""

import argparse
import fnmatch
import ftplib
import io
import sys

import h5py

SIGNATURE = b"\x89HDF\r\n\x1a\n"


# ---------------------------------------------------------------------------
# HDF5 superblock parsing + attribute recovery (same approach as
# recover_hdf5_attrs.py)
# ---------------------------------------------------------------------------

def parse_stored_eof(data: bytes) -> int:
    """Parse the HDF5 superblock and return the stored end-of-file address."""
    if len(data) < 12:
        raise ValueError("File too short to contain even a minimal superblock")

    if data[:8] != SIGNATURE:
        raise ValueError("Not an HDF5 file: missing signature in first 8 bytes")

    version = data[8]

    if version in (0, 1):
        size_of_offsets = data[13]
        offset = 24
        if version == 1:
            offset += 4
        offset += size_of_offsets  # skip base address
        offset += size_of_offsets  # skip free-space info address
        eof_bytes = data[offset:offset + size_of_offsets]

    elif version in (2, 3):
        size_of_offsets = data[9]
        offset = 12
        offset += size_of_offsets  # skip base address
        offset += size_of_offsets  # skip superblock extension address
        eof_bytes = data[offset:offset + size_of_offsets]

    else:
        raise ValueError(f"Unsupported/unknown superblock version: {version}")

    if len(eof_bytes) < size_of_offsets:
        raise ValueError("File too short to contain the superblock's EOF field")

    return int.from_bytes(eof_bytes, byteorder="little")


def recover_attrs(data: bytes, stored_eof: int):
    """Pad `data` to `stored_eof` bytes and read root attributes one at a time."""
    if len(data) < stored_eof:
        padded = data + b"\x00" * (stored_eof - len(data))
    else:
        padded = data

    recovered = {}
    failed = []

    with h5py.File(io.BytesIO(padded), "r") as f:
        try:
            names = list(f.attrs.keys())
        except Exception as e:
            return recovered, failed, f"Could not list attribute names: {e}"

        for name in names:
            try:
                recovered[name] = f.attrs[name]
            except Exception as e:
                failed.append((name, str(e)))

    return recovered, failed, None


# ---------------------------------------------------------------------------
# FTP handling
# ---------------------------------------------------------------------------

def list_files(ftp: ftplib.FTP, directory: str, pattern: str):
    """
    List regular files (not subdirectories) in `directory` matching
    `pattern`. Prefers MLSD (gives file/dir type) and falls back to
    NLST + a best-effort size check if MLSD isn't supported.
    """
    files = []
    try:
        for name, facts in ftp.mlsd(directory):
            if name in (".", ".."):
                continue
            if facts.get("type") == "file" and fnmatch.fnmatch(name, pattern):
                files.append(name)
    except ftplib.error_perm:
        # Server doesn't support MLSD; fall back to NLST.
        names = ftp.nlst(directory)
        for full in names:
            name = full.rsplit("/", 1)[-1]
            if fnmatch.fnmatch(name, pattern):
                files.append(name)
    return sorted(files)


def fetch_first_bytes(ftp: ftplib.FTP, remote_path: str, size: int) -> bytes:
    """
    Download only the first `size` bytes of `remote_path` over FTP,
    then abort the transfer rather than pulling the rest of the file.
    """
    data = bytearray()
    conn = ftp.transfercmd(f"RETR {remote_path}")
    try:
        while len(data) < size:
            chunk = conn.recv(min(4096, size - len(data)))
            if not chunk:
                break
            data.extend(chunk)
    finally:
        conn.close()

    # We almost certainly closed the data connection before the server
    # finished sending, so the control-channel response for RETR may
    # be an error or may hang. Try to clear it, but don't fail the
    # whole run if the server doesn't like the early abort.
    try:
        ftp.voidresp()
    except Exception:
        try:
            ftp.abort()
        except Exception:
            pass

    return bytes(data[:size])


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--server", default="lapalma3.iac.es", help="FTP server hostname")
    parser.add_argument("--user", default="anonymous", help="FTP username")
    parser.add_argument("--password", default="anonymous", help="FTP password")
    parser.add_argument("--port", type=int, default=21, help="FTP port (default: 21)")
    parser.add_argument("--directory", required=True, help="Remote directory to scan")
    parser.add_argument(
        "--size", type=int, default=4096,
        help="Number of bytes to download from the start of each file (default: 4096)",
    )
    parser.add_argument(
        "--pattern", default="*.h5",
        help="Glob pattern to filter filenames (default: *.h5; use '*' for all files)",
    )
    parser.add_argument(
        "--timeout", type=int, default=30, help="FTP connection timeout in seconds"
    )
    args = parser.parse_args()

    print(f"Connecting to {args.server}:{args.port} as {args.user!r}...")
    ftp = ftplib.FTP(timeout=args.timeout)
    try:
        ftp.connect(args.server, args.port)
        ftp.login(args.user, args.password)
    except Exception as e:
        print(f"Failed to connect/login: {e}")
        sys.exit(1)

    print(f"Listing {args.directory!r} for files matching {args.pattern!r}...")
    try:
        files = list_files(ftp, args.directory, args.pattern)
    except Exception as e:
        print(f"Failed to list directory: {e}")
        ftp.quit()
        sys.exit(1)

    if not files:
        print("No matching files found.")
        ftp.quit()
        return

    print(f"Found {len(files)} file(s). Downloading first {args.size} bytes of each...\n")

    for name in files:
        remote_path = f"{args.directory.rstrip('/')}/{name}"
        print(f"=== {name} ===")
        try:
            data = fetch_first_bytes(ftp, remote_path, args.size)
        except Exception as e:
            print(f"  Failed to download partial file: {e}\n")
            continue

        print(f"  Downloaded {len(data)} bytes")

        try:
            stored_eof = parse_stored_eof(data)
        except Exception as e:
            print(f"  Not readable as HDF5 (or too short): {e}\n")
            continue

        print(f"  Superblock reports original size: {stored_eof} bytes")

        recovered, failed, list_error = recover_attrs(data, stored_eof)

        if list_error:
            print(f"  {list_error}\n")
            continue

        if recovered:
            print(f"  Recovered {len(recovered)} attribute(s):")
            for k, v in recovered.items():
                print(f"    {k}: {v!r}")
        else:
            print("  No attributes could be recovered.")

        if failed:
            print(f"  {len(failed)} attribute(s) listed but failed to read:")
            for fname, err in failed:
                print(f"    {fname}: {err}")

        print()

    ftp.quit()


if __name__ == "__main__":
    main()
