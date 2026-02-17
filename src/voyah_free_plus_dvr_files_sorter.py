#!/usr/bin/env python3
"""
Voyah Free+ DVR "flat directory" organizer

What it does
------------
Given a directory that contains files like:

  DVR_LoopRecording_2026-02-11_02-30-23_back_camera.mp4
  DVR_LoopRecording_2026-02-11_02-30-23_front_camera.mp4
  DVR_LoopRecording_2026-02-11_02-30-23_left_camera.mp4
  DVR_LoopRecording_2026-02-11_02-30-23_right_camera.mp4

It creates folders like:

  2026-02-11_02-30-23/
  2026-02-11_02-31-23/
  ...

and moves files into the matching folder based on their embedded timestamp.

Safety features
---------------
- DRY_RUN mode (default True) shows what would happen without moving anything.
- Avoids overwriting: if destination file already exists, it can skip or rename.
- Only touches files that match the expected pattern.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path


# =============================================================================
# SETTINGS
# =============================================================================

# Directory that currently contains the flat list of MP4 files:
SOURCE_DIR = Path(r"D:/DRV_FLAT_VIDEOS")

# If True: do not move anything, only print actions.
DRY_RUN = False

# If a file with the same name already exists in destination:
#   - "skip": do nothing for that file
#   - "rename": move using a new name with suffix "_DUP1", "_DUP2", ...
ON_CONFLICT = "rename"  # "skip" or "rename"

# Only move files from this set of camera names (extra safety).
ALLOWED_CAMS = {"front", "back", "left", "right"}

# Expected filename pattern:
# DVR_LoopRecording_YYYY-MM-DD_HH-MM-SS_front_camera.mp4
FILE_RE = re.compile(
    r"^DVR_LoopRecording_"
    r"(?P<date>\d{4}-\d{2}-\d{2})_"
    r"(?P<time>\d{2}-\d{2}-\d{2})_"
    r"(?P<cam>front|back|left|right)_camera\.mp4$",
    re.IGNORECASE,
)


# =============================================================================
# Helpers
# =============================================================================

def log(msg: str) -> None:
    print(msg, flush=True)


def ensure_dir(p: Path) -> None:
    if not DRY_RUN:
        p.mkdir(parents=True, exist_ok=True)


def next_available_name(dest: Path) -> Path:
    """
    If dest exists, return a new path like:
      name_DUP1.mp4, name_DUP2.mp4, ...
    """
    if not dest.exists():
        return dest

    stem = dest.stem
    suffix = dest.suffix
    parent = dest.parent

    for i in range(1, 10_000):
        candidate = parent / f"{stem}_DUP{i}{suffix}"
        if not candidate.exists():
            return candidate

    raise RuntimeError(f"Could not find free destination name for: {dest}")


@dataclass(frozen=True)
class MovePlan:
    src: Path
    dst_dir: Path
    dst: Path


def build_move_plan(file_path: Path) -> MovePlan | None:
    """
    If file name matches expected DVR pattern, return move plan.
    Otherwise return None.
    """
    m = FILE_RE.match(file_path.name)
    if not m:
        return None

    date_s = m.group("date")
    time_s = m.group("time")
    cam = m.group("cam").lower()

    if cam not in ALLOWED_CAMS:
        return None

    folder_name = f"{date_s}_{time_s}"
    dst_dir = file_path.parent / folder_name
    dst = dst_dir / file_path.name

    # If it's already in the correct folder, skip.
    if file_path.parent.name == folder_name:
        return None

    return MovePlan(src=file_path, dst_dir=dst_dir, dst=dst)


def move_one(plan: MovePlan) -> str:
    """
    Execute (or simulate) a move. Returns a status string.
    """
    dst = plan.dst

    if dst.exists():
        if ON_CONFLICT == "skip":
            return f"SKIP (exists): {plan.src.name} -> {dst}"
        elif ON_CONFLICT == "rename":
            dst = next_available_name(dst)
        else:
            raise ValueError(f"Unknown ON_CONFLICT={ON_CONFLICT!r}")

    if DRY_RUN:
        return f"DRY:  move {plan.src.name} -> {dst.parent.name}/{dst.name}"

    ensure_dir(plan.dst_dir)
    shutil.move(str(plan.src), str(dst))
    return f"MOVED: {plan.src.name} -> {dst.parent.name}/{dst.name}"


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    if not SOURCE_DIR.exists() or not SOURCE_DIR.is_dir():
        raise SystemExit(f"SOURCE_DIR does not exist or is not a directory: {SOURCE_DIR}")

    log(f"SOURCE_DIR   = {SOURCE_DIR}")
    log(f"DRY_RUN      = {DRY_RUN}")
    log(f"ON_CONFLICT  = {ON_CONFLICT}")
    log("Scanning...")

    files = [p for p in SOURCE_DIR.iterdir() if p.is_file()]
    if not files:
        log("No files found.")
        return

    plans: list[MovePlan] = []
    ignored = 0

    for f in files:
        plan = build_move_plan(f)
        if plan is None:
            ignored += 1
            continue
        plans.append(plan)

    # Sort by destination folder then name (nice predictable output)
    plans.sort(key=lambda x: (x.dst_dir.name, x.src.name.lower()))

    log(f"Matched files: {len(plans)}")
    log(f"Ignored files: {ignored}")

    if not plans:
        log("Nothing to do.")
        return

    moved = 0
    skipped = 0

    # Create all destination directories up front (if not DRY_RUN)
    if not DRY_RUN:
        for d in sorted({p.dst_dir for p in plans}, key=lambda x: x.name):
            ensure_dir(d)

    for plan in plans:
        status = move_one(plan)
        log(status)
        if status.startswith("MOVED") or status.startswith("DRY:"):
            moved += 1
        elif status.startswith("SKIP"):
            skipped += 1

    log("")
    log(f"Done. planned/moved={moved}, skipped={skipped}, ignored={ignored}")

    if DRY_RUN:
        log("\nDRY_RUN is True -> no files were moved. Set DRY_RUN = False to apply changes.")


if __name__ == "__main__":
    main()
