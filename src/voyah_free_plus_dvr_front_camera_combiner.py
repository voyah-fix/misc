#!/usr/bin/env python3
"""
Voyah Free+ FRONT-camera extractor + bottom crop (170px)

What this script does
---------------------
Same folder scanning / grouping / timestamp logic as the 4-cam merger, but:
  1) Uses ONLY the front camera per segment (video + audio if present).
  2) Crops 170 px from the bottom (keeps full width).
     Input 1920x1080 -> output 1920x910 (even, encoder-friendly).
  3) Keeps the rest of the logic: timestamp overlay, paths, progress UX, per-date concat, etc.

Requirements
------------
- Python 3.10+
- ffmpeg + ffprobe accessible via PATH, or set explicit full paths below.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# =============================================================================
# SETTINGS
# =============================================================================

ROOT_DIR = Path(r"D:/DVR_INPUT")          # <-- where your timestamp folders live
OUT_DIR = Path(r"D:/DVR_OUTPUT_FRONT")    # <-- where outputs will be written

FFMPEG = "ffmpeg"      # e.g. r"C:\Tools\ffmpeg\bin\ffmpeg.exe"
FFPROBE = "ffprobe"    # e.g. r"C:\Tools\ffmpeg\bin\ffprobe.exe"

VERBOSE = True
SHOW_FFMPEG_CMD = True

# Folder name like: 2025-11-17_14-25-37
FOLDER_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})_(?P<time>\d{2}-\d{2}-\d{2})$"
)

# Expected file name pattern inside each folder
FILE_RE = re.compile(
    r"^DVR_LoopRecording_"
    r"(?P<date>\d{4}-\d{2}-\d{2})_"
    r"(?P<time>\d{2}-\d{2}-\d{2})_"
    r"(?P<cam>front|back|left|right)_camera\.mp4$",
    re.IGNORECASE,
)

OUTPUT_FPS = 30
CRF = 20
PRESET = "veryfast"

# Crop settings: cut 170px from bottom, keep full width.
CROP_BOTTOM_PX = 170

# Timestamp overlay
TIMESTAMP_SHIFT_HOURS = -5
DRAW_TIMESTAMP = True

TIMESTAMP_FONTFILE: str | None = None  # e.g. r"C:\Windows\Fonts\arial.ttf"
TIMESTAMP_FONT_FALLBACK_NAME = "Arial"

TIMESTAMP_FONT_SIZE = 50
TIMESTAMP_PADDING_X = 30
TIMESTAMP_PADDING_Y = 24
TIMESTAMP_BOX = True
TIMESTAMP_BOX_BORDER = 0
TIMESTAMP_BOX_OPACITY = 0.35

SKIP_EXISTING_SEGMENTS = False

FFMPEG_LOGLEVEL = "info"
HEARTBEAT_SEC = 10

TTY_MIN_INTERVAL_SEC = 0.5
NON_TTY_MIN_INTERVAL_SEC = 3.0
NON_TTY_MIN_PCT_STEP = 3.0


# =============================================================================
# Helpers
# =============================================================================

def log(msg: str) -> None:
    if VERBOSE:
        print(msg, flush=True)


def pct(i: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return (i / total) * 100.0


def fmt_pct(x: float) -> str:
    return f"{x:5.1f}%"


def is_real_tty() -> bool:
    if os.environ.get("PYCHARM_HOSTED") == "1":
        return False
    try:
        return sys.stdout.isatty()
    except Exception:
        return False


def parse_ffmpeg_kv_progress(line: str) -> tuple[str, str] | None:
    line = (line or "").strip()
    if not line or "=" not in line:
        return None
    k, v = line.split("=", 1)
    return k.strip(), v.strip()


def fmt_time_mmss(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "t=??:??"
    mm = int(seconds // 60)
    ss = int(seconds % 60)
    return f"t={mm:02d}:{ss:02d}"


def format_status_line(ctx: dict, file_pct: float | None, out_time_s: float | None) -> str:
    date_s = ctx.get("date", "?")
    d_i, d_t = ctx.get("date_idx", 0), ctx.get("date_total", 0)
    s_i, s_t = ctx.get("seg_idx", 0), ctx.get("seg_total", 0)
    stage = ctx.get("stage", "FFMPEG")
    name = ctx.get("name", "")

    dp = fmt_pct(pct(d_i, d_t))
    sp = fmt_pct(pct(s_i, s_t))

    file_pct_str = "  ?.?%" if file_pct is None else fmt_pct(file_pct)

    return (
        f"date {date_s} [{d_i}/{d_t} {dp}]"
        f" | seg [{s_i}/{s_t} {sp}]"
        f" | stage {stage}"
        f" | progress {file_pct_str} {fmt_time_mmss(out_time_s)} {name}"
    )


def run_live(cmd: list[str], check: bool = True, ctx: dict | None = None, duration_sec: float | None = None) -> int:
    ctx = ctx or {}

    name = ctx.get("name", "").strip()
    header_label = ctx.get("header_label", "Job")
    if name:
        print(f"\n>> {header_label}: {name}", flush=True)
    else:
        print(f"\n>> {header_label}", flush=True)

    if SHOW_FFMPEG_CMD:
        print("   CMD:", " ".join(shlex.quote(str(c)) for c in cmd), flush=True)

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    last_output = time.time()
    last_print_ts = 0.0
    last_printed_pct = -1e9

    tty = is_real_tty()
    min_interval = TTY_MIN_INTERVAL_SEC if tty else NON_TTY_MIN_INTERVAL_SEC

    out_time_s: float | None = None
    file_pct: float | None = None
    saw_any_progress = False

    def maybe_print(force: bool = False) -> None:
        nonlocal last_print_ts, last_printed_pct, file_pct, out_time_s
        now = time.time()
        if not force and (now - last_print_ts) < min_interval:
            return

        if out_time_s is not None and duration_sec and duration_sec > 0:
            file_pct = max(0.0, min(100.0, (out_time_s / duration_sec) * 100.0))

        if not tty and not force and file_pct is not None:
            if (file_pct - last_printed_pct) < NON_TTY_MIN_PCT_STEP:
                return

        line = format_status_line(ctx, file_pct, out_time_s)

        if tty:
            print(line.ljust(220), end="\r" if not force else "\n", flush=True)
        else:
            print(line, flush=True)

        last_print_ts = now
        if file_pct is not None:
            last_printed_pct = file_pct

    while True:
        line = p.stderr.readline() if p.stderr else ""
        if line:
            last_output = time.time()

            kv = parse_ffmpeg_kv_progress(line)
            if kv:
                saw_any_progress = True
                k, v = kv

                if k == "out_time_ms":
                    try:
                        out_time_ms = int(v)
                        out_time_s = out_time_ms / 1_000_000.0
                    except ValueError:
                        pass
                    maybe_print(force=False)

                elif k == "out_time" and out_time_s is None:
                    try:
                        parts = v.split(":")
                        if len(parts) >= 3:
                            hh = int(parts[0])
                            mm = int(parts[1])
                            ss = float(parts[2])
                            out_time_s = hh * 3600 + mm * 60 + ss
                    except Exception:
                        pass
                    maybe_print(force=False)

                elif k == "progress":
                    if v == "end" and tty:
                        maybe_print(force=True)

            else:
                t = line.strip()
                if not t:
                    continue
                low = t.lower()
                if "error" in low or "invalid" in low or "warning" in low or "fontconfig" in low:
                    print("[ffmpeg]", t, flush=True)

        else:
            if p.poll() is not None:
                break

            if time.time() - last_output >= HEARTBEAT_SEC:
                maybe_print(force=True)
                print(f"...still running (no output for {HEARTBEAT_SEC}s)...", flush=True)
                last_output = time.time()
            time.sleep(0.2)

    rc = p.wait()

    if saw_any_progress:
        if out_time_s is not None and duration_sec and duration_sec > 0:
            file_pct = max(0.0, min(100.0, (out_time_s / duration_sec) * 100.0))
        maybe_print(force=True)

    if check and rc != 0:
        raise subprocess.CalledProcessError(rc, cmd)
    return rc


def ffprobe_has_audio(path: Path) -> bool:
    cmd = [
        FFPROBE, "-v", "error",
        "-select_streams", "a",
        "-show_entries", "stream=index",
        "-of", "json",
        str(path),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if p.returncode != 0:
        return False
    try:
        data = json.loads(p.stdout or "{}")
        return bool(data.get("streams"))
    except json.JSONDecodeError:
        return False


def ffprobe_duration_sec(path: Path) -> float | None:
    cmd = [
        FFPROBE, "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=nw=1:nk=1",
        str(path),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if p.returncode != 0:
        return None
    try:
        return float((p.stdout or "").strip())
    except ValueError:
        return None


def parse_dt(date_s: str, time_s: str) -> datetime:
    return datetime.strptime(f"{date_s}_{time_s}", "%Y-%m-%d_%H-%M-%S")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_concat_list(file_paths: list[Path], out_txt: Path) -> None:
    lines: list[str] = []
    for p in file_paths:
        s = str(p.resolve()).replace("'", r"'\''")
        lines.append(f"file '{s}'")
    out_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")


def find_date_folders(root: Path) -> dict[str, list[Path]]:
    grouped: dict[str, list[tuple[datetime, Path]]] = {}

    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        m = FOLDER_RE.match(entry.name)
        if not m:
            continue
        date_s = m.group("date")
        time_s = m.group("time")
        grouped.setdefault(date_s, []).append((parse_dt(date_s, time_s), entry))

    out: dict[str, list[Path]] = {}
    for date_s, items in grouped.items():
        items.sort(key=lambda x: x[0])
        out[date_s] = [p for _, p in items]
    return out


def find_camera_files(folder: Path) -> dict[str, Path]:
    cams: dict[str, Path] = {}
    for f in folder.iterdir():
        if not f.is_file():
            continue
        m = FILE_RE.match(f.name)
        if not m:
            continue
        cam = m.group("cam").lower()
        cams[cam] = f
    return cams


def print_ffmpeg_version() -> None:
    try:
        p = subprocess.run([FFMPEG, "-version"], capture_output=True, text=True, encoding="utf-8", errors="replace")
        if p.stdout:
            print("\n=== ffmpeg -version ===")
            print(p.stdout.splitlines()[0])
            print("======================\n")
    except Exception as e:
        print(f"Could not run ffmpeg -version: {e}")


def pick_windows_fontfile() -> str | None:
    if TIMESTAMP_FONTFILE:
        p = Path(TIMESTAMP_FONTFILE)
        if p.exists():
            return str(p).replace("\\", "/")

    windir = os.environ.get("WINDIR", r"C:\Windows")
    fonts_dir = Path(windir) / "Fonts"

    candidates = [
        fonts_dir / "arial.ttf",
        fonts_dir / "Arial.ttf",
        fonts_dir / "calibri.ttf",
        fonts_dir / "Calibri.ttf",
        fonts_dir / "segoeui.ttf",
        fonts_dir / "SegoeUI.ttf",
        fonts_dir / "tahoma.ttf",
        fonts_dir / "Tahoma.ttf",
    ]

    for c in candidates:
        if c.exists():
            return str(c).replace("\\", "/")

    return None


def build_overlay_filter_dynamic(folder_name: str) -> str | None:
    if not DRAW_TIMESTAMP:
        return None

    m = FOLDER_RE.match(folder_name)
    if not m:
        return None

    dt_src = parse_dt(m.group("date"), m.group("time"))
    dt_adj = dt_src + timedelta(hours=TIMESTAMP_SHIFT_HOURS)

    date_text = dt_adj.strftime("%Y-%m-%d")
    base_sec = dt_adj.hour * 3600 + dt_adj.minute * 60 + dt_adj.second

    fontfile = pick_windows_fontfile()
    if fontfile:
        ff_font = fontfile.replace("\\", "/").replace(":", r"\:")
        font_opt = f"fontfile='{ff_font}':"
    else:
        font_opt = f"font='{TIMESTAMP_FONT_FALLBACK_NAME}':"

    box = "1" if TIMESTAMP_BOX else "0"
    boxcolor = f"black@{TIMESTAMP_BOX_OPACITY}" if TIMESTAMP_BOX else "black@0"
    y = f"h-text_h-{TIMESTAMP_PADDING_Y}"

    H = f"trunc((t+{base_sec})/3600)"
    M = f"mod(trunc((t+{base_sec})/60)\\,60)"
    S = f"mod(trunc(t+{base_sec})\\,60)"
    F = f"mod(n\\,{OUTPUT_FPS})"

    def two_digits(expr: str) -> str:
        return (
            f"%{{eif\\:trunc(({expr})/10)\\:d}}"
            f"%{{eif\\:mod(({expr})\\,10)\\:d}}"
        )

    text_expr = (
        f"{date_text} "
        f"{two_digits(H)}\\:"
        f"{two_digits(M)}\\:"
        f"{two_digits(S)}"
        f"x"
        f"{two_digits(F)}"
    )

    draw = (
        "drawtext="
        f"{font_opt}"
        f"text='{text_expr}':"
        f"fontsize={TIMESTAMP_FONT_SIZE}:"
        "fontcolor=white:"
        "shadowcolor=black:shadowx=3:shadowy=3:"
        f"box={box}:"
        f"boxcolor={boxcolor}:"
        f"boxborderw={TIMESTAMP_BOX_BORDER}:"
        "x=(w-text_w)/2:"
        f"y={y}"
    )

    return draw


# =============================================================================
# Segment build (FRONT only)
# =============================================================================

def build_segment_front(
        folder: Path,
        front_file: Path,
        out_seg: Path,
        date_s: str,
        date_idx: int,
        date_total: int,
        seg_idx: int,
        seg_total: int,
) -> None:
    """
    Build one segment from FRONT camera only:
      - CFR timeline (setpts=N/(FPS*TB))
      - crop bottom CROP_BOTTOM_PX
      - optional timestamp overlay
      - audio: keep if exists, else synthesize silence
      - encode H.264 + AAC
    """

    has_audio = ffprobe_has_audio(front_file)

    # Progress estimation
    seg_dur = ffprobe_duration_sec(front_file) or 60.0
    duration_for_pct = seg_dur

    overlay = build_overlay_filter_dynamic(folder.name)

    # Crop math: keep full width, reduce height by CROP_BOTTOM_PX, anchor at top (y=0).
    # Example: 1080 - 170 = 910.
    crop_h_expr = f"ih-{CROP_BOTTOM_PX}"
    crop_filter = f"crop=iw:{crop_h_expr}:0:0"

    # Build filter chain
    # - setpts: stabilizes timestamps to desired FPS
    # - crop: removes bottom
    # - setsar=1: normalize SAR
    fg = [
        f"[0:v]setpts=N/({OUTPUT_FPS}*TB),{crop_filter},setsar=1[v0]"
    ]

    if overlay:
        fg.append(f"[v0]{overlay}[vout]")
    else:
        fg.append("[v0]copy[vout]")

    filtergraph = ";".join(fg)

    cmd: list[str] = [
        FFMPEG, "-y",
        "-hide_banner",
        "-loglevel", FFMPEG_LOGLEVEL,
        "-stats",
        "-progress", "pipe:2",
        "-fflags", "+genpts",
        "-avoid_negative_ts", "make_zero",
        "-i", str(front_file),
    ]

    # If no audio, add silent audio as extra input.
    if not has_audio:
        cmd += ["-f", "lavfi", "-t", f"{seg_dur:.3f}", "-i", "anullsrc=r=48000:cl=stereo"]

    cmd += ["-filter_complex", filtergraph, "-map", "[vout]"]

    if has_audio:
        cmd += ["-map", "0:a:0?"]
    else:
        cmd += ["-map", "1:a:0"]

    cmd += ["-r", str(OUTPUT_FPS)]
    cmd += ["-shortest"]

    cmd += [
        "-c:v", "libx264",
        "-preset", PRESET,
        "-crf", str(CRF),
        "-pix_fmt", "yuv420p",
        "-profile:v", "high",
        "-level:v", "4.2",   # plenty for 1920x910@30
        "-c:a", "aac",
        "-b:a", "160k",
        str(out_seg),
    ]

    ctx = {
        "stage": "ENCODE",
        "header_label": "Date-time",
        "name": folder.name,
        "date": date_s,
        "date_idx": date_idx,
        "date_total": date_total,
        "seg_idx": seg_idx,
        "seg_total": seg_total,
    }

    t0 = time.time()
    run_live(cmd, check=True, ctx=ctx, duration_sec=duration_for_pct)
    dt = time.time() - t0

    if VERBOSE:
        print(f"done in {dt:.1f}s -> {out_seg.name}", flush=True)


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    print_ffmpeg_version()
    ensure_dir(OUT_DIR)

    log(f"ROOT_DIR = {ROOT_DIR}")
    log(f"OUT_DIR  = {OUT_DIR}")
    log(f"FFMPEG   = {FFMPEG}")
    log(f"FFPROBE  = {FFPROBE}")
    log(f"OUTPUT_FPS = {OUTPUT_FPS} (CFR)")
    log(f"CROP_BOTTOM_PX = {CROP_BOTTOM_PX} (output height = ih-{CROP_BOTTOM_PX})")
    log("Scanning for timestamp folders...")

    if DRAW_TIMESTAMP:
        ff = pick_windows_fontfile()
        if ff:
            log(f"Timestamp fontfile: {ff}")
        else:
            log("WARNING: No suitable fontfile found; drawtext may fail on this ffmpeg build.")
            log("         Fix: set TIMESTAMP_FONTFILE to a real .ttf path or disable DRAW_TIMESTAMP.")

    date_to_folders = find_date_folders(ROOT_DIR)
    if not date_to_folders:
        print(f"No folders like YYYY-MM-DD_HH-MM-SS found in: {ROOT_DIR}")
        return

    dates = sorted(date_to_folders.items())
    date_total = len(dates)
    log(f"Found dates: {date_total}")

    for date_idx, (date_s, folders) in enumerate(dates, start=1):
        work_dir = OUT_DIR / f"_work_{date_s}"
        ensure_dir(work_dir)

        if VERBOSE:
            print(
                f"\nProcessing date {date_s} [{date_idx}/{date_total} {fmt_pct(pct(date_idx, date_total))}] "
                f"({len(folders)} folders found)",
                flush=True,
            )

        # Identify “complete” segments for FRONT only.
        ready: list[tuple[Path, Path]] = []
        skipped = 0

        for folder in folders:
            cams = find_camera_files(folder)
            front = cams.get("front")
            if not front:
                skipped += 1
                continue
            ready.append((folder, front))

        seg_total = len(ready)
        print(f"  segments ready: {seg_total}, skipped (no front): {skipped}", flush=True)

        if seg_total == 0:
            print(f"Skipping {date_s}: no front segments found.", flush=True)
            continue

        segment_files: list[Path] = []
        built = 0
        reused = 0

        for seg_idx, (folder, front_file) in enumerate(ready, start=1):
            out_seg = work_dir / f"seg_{folder.name}.mp4"

            if SKIP_EXISTING_SEGMENTS and out_seg.exists() and out_seg.stat().st_size > 0:
                reused += 1
                print(f"\n>> Date-time: {folder.name}", flush=True)
                print(
                    f"date {date_s} [{date_idx}/{date_total} {fmt_pct(pct(date_idx, date_total))}]"
                    f" | seg [{seg_idx}/{seg_total} {fmt_pct(pct(seg_idx, seg_total))}]"
                    f" | stage REUSE"
                    f" | progress {fmt_pct(100.0)} t=00:00 {folder.name}",
                    flush=True,
                )
                segment_files.append(out_seg)
                continue

            build_segment_front(
                folder=folder,
                front_file=front_file,
                out_seg=out_seg,
                date_s=date_s,
                date_idx=date_idx,
                date_total=date_total,
                seg_idx=seg_idx,
                seg_total=seg_total,
            )
            built += 1
            segment_files.append(out_seg)

        print(
            f"  segments: total={len(segment_files)}, built={built}, reused={reused}, skipped={skipped}",
            flush=True,
        )

        # Concatenate segments into a single file for the date (stream copy).
        concat_list = work_dir / f"concat_segments_{date_s}.txt"
        write_concat_list(segment_files, concat_list)

        out_mp4 = OUT_DIR / f"{date_s}_FRONT_CROP170.mp4"

        concat_durations: list[float] = []
        for sf in segment_files:
            d = ffprobe_duration_sec(sf)
            if d and d > 0:
                concat_durations.append(d)
        concat_total_dur = sum(concat_durations) if concat_durations else None

        cmd = [
            FFMPEG, "-y",
            "-hide_banner",
            "-loglevel", "info",
            "-stats",
            "-progress", "pipe:2",
            "-f", "concat",
            "-safe", "0",
            "-i", str(concat_list),
            "-c", "copy",
            "-movflags", "+faststart",
            str(out_mp4),
        ]

        ctx = {
            "stage": "CONCAT",
            "header_label": "Date",
            "name": date_s,
            "date": date_s,
            "date_idx": date_idx,
            "date_total": date_total,
            "seg_idx": seg_total,
            "seg_total": seg_total,
        }

        run_live(cmd, check=True, ctx=ctx, duration_sec=concat_total_dur)
        print(f"Created: {out_mp4}", flush=True)


if __name__ == "__main__":
    main()
