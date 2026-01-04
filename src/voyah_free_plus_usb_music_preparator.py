#!/usr/bin/env python3
"""
USB Music Flattener for dumb Voyah Free+ 'USB Music' app

This utility builds a *single-level* folder layout by converting nested paths into
flattened folder names.

What it does
------------
1) Recursively scans SRC_ROOT (unlimited nesting).
2) Flattens folder hierarchy into a single-level folder structure:

      SRC:  Genesis/Greatest Hits/CD1/some-song.mp3
      DST:  Genesis - Greatest Hits - CD1/some-song.mp3

3) Optionally fills MP3 ID3 tags if they are missing/blank, using best-effort inference
   from folder structure and filename:
   - artist = first folder
   - album  = second folder
   - discnumber = inferred from folder names like "CD1", "Disc 2"
   - tracknumber/title = inferred from filename patterns like "01 - Song Name.mp3"

4) DRY_RUN is enabled by default: no filesystem writes and no tag edits.

Dependencies
------------
  pip install mutagen

Supported formats
-----------------
- Tag reading/writing: MP3 only (ID3 via mutagen).
- File flattening/copying: any extension in INCLUDE_EXTS can be copied/moved,
  but tag filling is only performed for .mp3 files.
"""

from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

# =============================================================================
# CONFIG
# =============================================================================

SRC_ROOT = Path(r"D:/MUSIC_SRC")  # root of your nested music library (source)
DST_ROOT = Path(r"D:/MUSIC_FLAT")  # destination root for flattened structure

# SAFETY DEFAULT: True
# - True  => prints planned actions only; no file writes; no tag edits
# - False => performs copy/move + tag edits (if enabled)
DRY_RUN = True

# Copy vs move (ignored in DRY_RUN mode)
COPY_MODE = True  # True = copy, False = move

# Extensions to include in flattening/copying.
# NOTE: Tag filling is MP3-only. Non-MP3 files are still copied/moved.
INCLUDE_EXTS = {".mp3", ".flac"}

# If destination file already exists:
# - True  => auto-rename to " (2)", " (3)", ...
# - False => skip that file
RENAME_ON_CONFLICT = True

# Tag filling policy (MP3-only): fill only if tag is missing/blank
FILL_ARTIST = True
FILL_ALBUM = True
FILL_TITLE = True
FILL_TRACKNUMBER = True
FILL_DISCNUMBER = True

# Folder names considered "noise" in paths (rarely needed, mostly defensive)
IGNORE_FOLDER_NAMES = {"", ".", ".."}

# Windows/FAT-ish invalid filename chars; also trims trailing dots/spaces (Windows quirk)
INVALID_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
WHITESPACE_RE = re.compile(r"\s+")

# =============================================================================
# Dependency import (mutagen)
# =============================================================================
try:
    from mutagen.id3 import ID3, ID3NoHeaderError
    from mutagen.easyid3 import EasyID3
except Exception as e:
    raise SystemExit(
        "Missing dependency: mutagen\n"
        "Install it with: pip install mutagen\n"
        f"Original import error: {e}"
    )


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class InferredTags:
    # Tags inferred from path/filename. Values may be None if inference failed.
    artist: Optional[str] = None
    album: Optional[str] = None
    title: Optional[str] = None
    tracknumber: Optional[str] = None
    discnumber: Optional[str] = None


# =============================================================================
# Utility helpers
# =============================================================================

def normalize_component(s: str) -> str:
    # Sanitize folder/file name components to be safe on Windows and FAT-like filesystems:
    # - Remove invalid characters
    # - Collapse whitespace
    # - Trim trailing dots/spaces (Windows does not like them)
    # - Never return empty: fallback to "Unknown"

    s = s.strip()
    s = INVALID_CHARS_RE.sub(" ", s)
    s = WHITESPACE_RE.sub(" ", s).strip()
    s = s.rstrip(" .")
    return s or "Unknown"


def is_blank(v: Optional[str]) -> bool:
    # Treat None, empty string, and whitespace-only as blank.
    return v is None or str(v).strip() == ""


def parse_track_and_title_from_filename(stem: str) -> Tuple[Optional[str], str]:
    # Try to parse track number and title from the filename (without extension).
    #
    # Examples:
    #   "01 - Song Name" -> track="01", title="Song Name"
    #   "1. Song Name"   -> track="1",  title="Song Name"
    #   "Song Name"      -> track=None, title="Song Name"

    s = stem.strip()

    # Common patterns with separators like "-", ".", "_", ")"
    m = re.match(r"^\s*(\d{1,3})\s*[-._)]\s*(.+?)\s*$", s)
    if m:
        return m.group(1), m.group(2).strip()

    # Pattern with just whitespace after a number: "01 Song Name"
    m = re.match(r"^\s*(\d{1,3})\s+(.+?)\s*$", s)
    if m:
        return m.group(1), m.group(2).strip()

    return None, s


def parse_disc_from_folder_name(name: str) -> Optional[str]:
    # Try to infer disc number from a folder name.
    #
    # Examples:
    #   "CD1", "CD 1", "Disc 2", "Disk 03" -> "1", "2", "03"

    s = name.strip()
    m = re.search(r"\b(cd|disc|disk)\s*0*(\d{1,2})\b", s, flags=re.IGNORECASE)
    if m:
        return m.group(2)
    return None


# =============================================================================
# Tag inference & editing (MP3 only)
# =============================================================================

def infer_tags_from_path(file_path: Path, src_root: Path) -> InferredTags:
    # Best-effort heuristic inference from path:
    #
    #   relative parts: [artist]/[album]/[...disc...]/file.mp3
    #
    # Rules:
    #   - artist = first folder
    #   - album  = second folder
    #   - discnumber = first match among remaining folders (CD/Disc/Disk N)
    #   - title & tracknumber = from filename
    #
    # Notes:
    #   - This is intentionally conservative; we prefer "something plausible"
    #     rather than complicated guesswork.
    #   - Returned values are raw (not normalized); normalization happens at write time.

    rel = file_path.relative_to(src_root)
    parts = [p for p in rel.parts[:-1] if p not in IGNORE_FOLDER_NAMES]

    artist = parts[0] if len(parts) >= 1 else None
    album = parts[1] if len(parts) >= 2 else None

    disc = None
    if len(parts) >= 3:
        for p in parts[2:]:
            disc = parse_disc_from_folder_name(p)
            if disc:
                break

    track, title = parse_track_and_title_from_filename(file_path.stem)

    return InferredTags(
        artist=artist,
        album=album,
        title=title,
        tracknumber=track,
        discnumber=disc,
    )


def read_easyid3(mp3_path: Path) -> EasyID3:
    # Load EasyID3 for an MP3 file.
    # If the file has no ID3 header, create one so tags can be written.

    try:
        return EasyID3(str(mp3_path))
    except ID3NoHeaderError:
        id3 = ID3()
        id3.save(str(mp3_path))
        return EasyID3(str(mp3_path))


def needs_fill(easy: EasyID3) -> bool:
    # Simple "empty tags" check:
    # return True if ALL common fields are missing/blank.

    fields = ["artist", "album", "title", "tracknumber", "discnumber"]
    for f in fields:
        if f in easy and any(str(x).strip() for x in easy.get(f, [])):
            return False
    return True


def fill_tags_if_needed(mp3_path: Path, inferred: InferredTags) -> None:
    # Fill tags only if they are missing/blank (MP3 only).
    #
    # We *never* overwrite existing non-blank values.
    # We write only fields that:
    #   - are enabled by FILL_* flags
    #   - have an inferred value
    #   - are currently missing/blank in the file

    easy = read_easyid3(mp3_path)

    def set_if_blank(field: str, value: Optional[str]) -> None:
        if value is None:
            return
        current = easy.get(field, [])
        if not current or all(is_blank(str(x)) for x in current):
            # normalize here to avoid illegal characters in tags
            easy[field] = [normalize_component(str(value))]

    if FILL_ARTIST:
        set_if_blank("artist", inferred.artist)
    if FILL_ALBUM:
        set_if_blank("album", inferred.album)
    if FILL_TITLE:
        set_if_blank("title", inferred.title)
    if FILL_TRACKNUMBER:
        set_if_blank("tracknumber", inferred.tracknumber)
    if FILL_DISCNUMBER:
        set_if_blank("discnumber", inferred.discnumber)

    easy.save()


# =============================================================================
# Flattening logic
# =============================================================================

def build_flat_folder_name(file_path: Path, src_root: Path) -> str:
    # Convert nested folders into a single flattened folder name.
    #
    # Example:
    #   "Genesis/Greatest Hits/CD1/file.mp3" -> "Genesis - Greatest Hits - CD1"
    #
    # Implementation:
    #   - Take all relative folders between src_root and the file
    #   - Normalize each component
    #   - Join with " - "

    rel = file_path.relative_to(src_root)
    folder_parts = [p for p in rel.parts[:-1] if p not in IGNORE_FOLDER_NAMES]
    if not folder_parts:
        folder_parts = ["Unknown"]
    folder_parts = [normalize_component(p) for p in folder_parts]
    return " - ".join(folder_parts)


def resolve_conflict(path: Path) -> Optional[Path]:
    # Resolve destination file name conflicts.
    #
    # Returns:
    #   - The same path if it doesn't exist
    #   - A renamed path "name (2).ext", "name (3).ext", ... if RENAME_ON_CONFLICT=True
    #   - None if skipping due to conflict and RENAME_ON_CONFLICT=False

    if not path.exists():
        return path
    if not RENAME_ON_CONFLICT:
        return None

    base = path.stem
    ext = path.suffix
    parent = path.parent
    i = 2
    while True:
        candidate = parent / f"{base} ({i}){ext}"
        if not candidate.exists():
            return candidate
        i += 1


def ensure_dir(p: Path) -> None:
    # Create directory path unless DRY_RUN is enabled.
    if DRY_RUN:
        return
    p.mkdir(parents=True, exist_ok=True)


def copy_or_move(src: Path, dst: Path) -> None:
    # Copy or move a file to destination.
    # No-op in DRY_RUN mode.

    if DRY_RUN:
        return
    ensure_dir(dst.parent)
    if COPY_MODE:
        shutil.copy2(src, dst)  # preserves timestamps/metadata when possible
    else:
        shutil.move(src, dst)


def should_try_tagging(path: Path) -> bool:
    # We only read/write ID3 tags for MP3 files.
    return path.suffix.lower() == ".mp3"


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    if not SRC_ROOT.exists():
        raise SystemExit(f"SRC_ROOT does not exist: {SRC_ROOT}")

    # Collect files first so we can print a stable total count.
    media_files: list[Path] = []
    for root, _, files in os.walk(SRC_ROOT):
        for fn in files:
            p = Path(root) / fn
            if p.suffix.lower() in INCLUDE_EXTS:
                media_files.append(p)

    print(f"Found {len(media_files)} file(s) under: {SRC_ROOT}")
    print(f"Destination root: {DST_ROOT}")
    print(f"DRY_RUN={DRY_RUN}, MODE={'copy' if COPY_MODE else 'move'}")
    print(f"INCLUDE_EXTS={sorted(INCLUDE_EXTS)}")
    print("-" * 80)

    file_ops_planned = 0
    skipped = 0
    tags_attempted = 0
    tag_fail = 0

    total = len(media_files)

    for i, src in enumerate(media_files, start=1):
        rel = src.relative_to(SRC_ROOT)

        # Destination folder is the flattened form of the relative folder chain.
        flat_folder = build_flat_folder_name(src, SRC_ROOT)
        dst_dir = DST_ROOT / flat_folder

        # Destination file name is sanitized to avoid illegal chars.
        dst_file = dst_dir / normalize_component(src.name)

        # Handle conflicts (existing destination file).
        dst_final = resolve_conflict(dst_file)
        if dst_final is None:
            print(f"[{i}/{total}] SKIP (exists): {rel} -> {dst_file.relative_to(DST_ROOT)}")
            skipped += 1
            continue

        # --- Optional tag filling (MP3 only) ---
        if should_try_tagging(src):
            inferred = infer_tags_from_path(src, SRC_ROOT)
            try:
                # Decide whether to fill:
                # - if all common fields are empty, OR
                # - if any of these fields are missing/blank (we fill only missing ones)
                easy = read_easyid3(src)
                fields = ["artist", "album", "title", "tracknumber", "discnumber"]
                will_fill = needs_fill(easy) or any(
                    (f not in easy) or all(is_blank(str(x)) for x in easy.get(f, []))
                    for f in fields
                )

                if will_fill:
                    tags_attempted += 1
                    if DRY_RUN:
                        print(
                            f"[{i}/{total}] TAG (dry): {rel}\n"
                            f"    inferred: artist={inferred.artist!r}, album={inferred.album!r}, "
                            f"disc={inferred.discnumber!r}, track={inferred.tracknumber!r}, title={inferred.title!r}"
                        )
                    else:
                        fill_tags_if_needed(src, inferred)

            except Exception as e:
                tag_fail += 1
                print(f"[{i}/{total}] TAG FAIL: {rel} ({e})")

        # --- Copy/move into flattened structure ---
        action = "COPY" if COPY_MODE else "MOVE"
        if DRY_RUN:
            print(f"[{i}/{total}] {action} (dry): {rel} -> {dst_final.relative_to(DST_ROOT)}")
        else:
            ensure_dir(dst_final.parent)
            copy_or_move(src, dst_final)
            print(f"[{i}/{total}] {action}: {rel} -> {dst_final.relative_to(DST_ROOT)}")

        file_ops_planned += 1

    print("-" * 80)
    print("Done.")
    print(f"Planned file ops: {file_ops_planned} | skipped: {skipped}")
    print(f"Tag operations (attempted): {tags_attempted} | tag failures: {tag_fail}")
    if DRY_RUN:
        print("\nNOTE: DRY_RUN=True, so nothing was changed. Set DRY_RUN=False to apply changes.")


if __name__ == "__main__":
    main()
