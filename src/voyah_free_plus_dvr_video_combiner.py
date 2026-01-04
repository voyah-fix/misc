#!/usr/bin/env python3
"""
Voyah Free+ 4-camera DVR videos merger
Results example: https://github.com/voyah-fix/misc/blob/main/media/voyah_free_plus_dvr_video_combiner_example.png

What this script does
---------------------
The DVR records four separate MP4 files per “segment” (front/back/left/right),
stored in folders named like:

    2025-11-17_14-25-37/

and files named like:

    DVR_LoopRecording_2025-11-17_14-25-37_front_camera.mp4
    DVR_LoopRecording_2025-11-17_14-25-37_back_camera.mp4
    DVR_LoopRecording_2025-11-17_14-25-37_left_camera.mp4
    DVR_LoopRecording_2025-11-17_14-25-37_right_camera.mp4

For every segment folder with all four cameras present, we:
  1) Pad each video to 1920x1080 (no scaling, only padding) and create a 2x2 tile:
        top row:    front | back
        bottom row: left  | right
     Output per segment: 3840x2160 (4K)
  2) Optionally overlay a timestamp at the bottom center.
     Timestamp is derived from the folder name and adjusted by a constant timezone shift.
  3) Pick audio from the first camera stream that actually has audio, otherwise generate silent audio.
  4) Encode each segment to H.264 + AAC.
  5) Concatenate all segment outputs for each date into a final MP4 with stream copy.

Requirements
------------
- Python 3.10+ (uses X | None type syntax)
- ffmpeg + ffprobe accessible via PATH, or set explicit full paths below.

Tested assumptions
------------------
- Segment folders follow the YYYY-MM-DD_HH-MM-SS naming.
- Each segment folder contains exactly one file per camera matching FILE_RE.
- ffmpeg supports:
    - drawtext filter (for timestamp)
    - libx264 encoder
    - concat demuxer (for day concatenation)

If your ffmpeg build lacks drawtext or has fontconfig issues on Windows:
- Provide TIMESTAMP_FONTFILE explicitly, or set DRAW_TIMESTAMP = False.
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

# Change to your real DVR input folder and output folder.
ROOT_DIR = Path(r"D:/DVR_INPUT")         # <-- where your timestamp folders live
OUT_DIR = Path(r"D:/DVR_OUTPUT_MERGED")  # <-- where merged outputs will be written

# If ffmpeg/ffprobe are not in PATH, provide full paths here.
FFMPEG = "ffmpeg"  # e.g. r"C:\Tools\ffmpeg\bin\ffmpeg.exe"
FFPROBE = "ffprobe"  # e.g. r"C:\Tools\ffmpeg\bin\ffprobe.exe"

# Logging / verbosity knobs
VERBOSE = True
SHOW_FFMPEG_CMD = True  # prints full ffmpeg command lines (can be long)

# Folder name like: 2025-11-17_14-25-37
FOLDER_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})_(?P<time>\d{2}-\d{2}-\d{2})$"
)

# Expected file name pattern inside each folder, example:
# DVR_LoopRecording_2025-11-17_14-25-37_front_camera.mp4
FILE_RE = re.compile(
    r"^DVR_LoopRecording_"
    r"(?P<date>\d{4}-\d{2}-\d{2})_"
    r"(?P<time>\d{2}-\d{2}-\d{2})_"
    r"(?P<cam>front|back|left|right)_camera\.mp4$",
    re.IGNORECASE,
)

# Output controls for per-segment encoding
OUTPUT_FPS = 30
CRF = 20
PRESET = "veryfast"

# No scaling. Pad each ~1900x1080 -> 1920x1080, then stack => 3840x2160 (4K).
TILE_W, TILE_H = 1920, 1080

# Timestamp overlay controls
# The folder timestamp might be recorded in a different timezone than you want to display.
# Example: DVR writes “UTC+8” (China time) but you want “UTC+3” => shift by -5 hours.
TIMESTAMP_SHIFT_HOURS = -5
DRAW_TIMESTAMP = True

# IMPORTANT (Windows): providing a real font file is the most reliable way
# to avoid fontconfig errors and “font name printed on video” issues.
# If None, we auto-pick a common Windows font, if available.
TIMESTAMP_FONTFILE: str | None = None  # e.g. r"C:\Windows\Fonts\arial.ttf"
TIMESTAMP_FONT_FALLBACK_NAME = "Arial"  # used only if no fontfile found (may fail on some builds)

TIMESTAMP_FONT_SIZE = 100
TIMESTAMP_PADDING_X = 30
TIMESTAMP_PADDING_Y = 24
TIMESTAMP_BOX = True
TIMESTAMP_BOX_BORDER = 0
TIMESTAMP_BOX_OPACITY = 0.35  # 0..1 (box only)

# If True: reuse already built segment files (skips re-encoding segments)
SKIP_EXISTING_SEGMENTS = False

# ffmpeg controls
FFMPEG_LOGLEVEL = "info"
HEARTBEAT_SEC = 10

# Progress throttling
# When output is a real terminal (TTY), we can update frequently on one line (carriage return).
# When not TTY (logs, pipes, IDE consoles), we print less often to avoid spam.
TTY_MIN_INTERVAL_SEC = 0.5
NON_TTY_MIN_INTERVAL_SEC = 3.0
NON_TTY_MIN_PCT_STEP = 3.0  # print only if progress advanced by >= this many % (non-tty)


# =============================================================================
# Helpers
# =============================================================================

def log(msg: str) -> None:
    # Print only when VERBOSE is enabled.
    if VERBOSE:
        print(msg, flush=True)


def pct(i: int, total: int) -> float:
    # Percentage helper that won't crash on total=0.
    if total <= 0:
        return 0.0
    return (i / total) * 100.0


def fmt_pct(x: float) -> str:
    # Format percentage in a fixed-width nice-looking way.
    return f"{x:5.1f}%"


def is_real_tty() -> bool:
    # Decide whether we can do '\r' in-place updates.
    # Some IDE consoles report isatty() but behave poorly with carriage returns.
    # PyCharm sets this env var; its console often isn't a true TTY.

    if os.environ.get("PYCHARM_HOSTED") == "1":
        return False
    try:
        return sys.stdout.isatty()
    except Exception:
        return False


def parse_ffmpeg_kv_progress(line: str) -> tuple[str, str] | None:
    # ffmpeg with "-progress pipe:2" writes "key=value" lines to stderr.
    # We parse those into (key, value) pairs.

    line = (line or "").strip()
    if not line or "=" not in line:
        return None
    k, v = line.split("=", 1)
    return k.strip(), v.strip()


def fmt_time_mmss(seconds: float | None) -> str:
    #Human-friendly t=MM:SS for a status line.
    if seconds is None or seconds < 0:
        return "t=??:??"
    mm = int(seconds // 60)
    ss = int(seconds % 60)
    return f"t={mm:02d}:{ss:02d}"


def format_status_line(ctx: dict, file_pct: float | None, out_time_s: float | None) -> str:
    # Build a single compact status line with:
    # - date progress (date index out of total)
    # - segment progress (segment index out of total)
    # - stage (ENCODE / CONCAT / REUSE / etc.)
    # - percent within the current file
    # - output timestamp (MM:SS)

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
    # Run a subprocess while:
    # - optionally printing the full command (debug)
    # - parsing ffmpeg progress ("-progress pipe:2")
    # - emitting a throttled, human-friendly status line
    #
    # IMPORTANT:
    # - We read stderr line-by-line because "-progress pipe:2" uses stderr.
    # - Some ffmpeg builds emit other diagnostics to stderr as well.
    #
    # Args:
    #     cmd: command list (already split, no shell=True)
    #     check: if True, non-zero exit code raises CalledProcessError
    #     ctx: context dict used for status line and headings
    #     duration_sec: if provided, we estimate percent based on out_time / duration

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
        bufsize=1,  # line-buffered
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
        # Throttle printing so logs remain readable.
        nonlocal last_print_ts, last_printed_pct, file_pct, out_time_s
        now = time.time()
        if not force and (now - last_print_ts) < min_interval:
            return

        # Estimate percent from out_time vs known duration, if possible.
        if out_time_s is not None and duration_sec and duration_sec > 0:
            file_pct = max(0.0, min(100.0, (out_time_s / duration_sec) * 100.0))

        # For non-TTY contexts, print only if percent meaningfully advanced.
        if not tty and not force and file_pct is not None:
            if (file_pct - last_printed_pct) < NON_TTY_MIN_PCT_STEP:
                return

        line = format_status_line(ctx, file_pct, out_time_s)

        if tty:
            # Carriage return updates the same line.
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
                    # Most reliable timing field (microseconds encoded as integer).
                    try:
                        out_time_ms = int(v)
                        out_time_s = out_time_ms / 1_000_000.0
                    except ValueError:
                        pass
                    maybe_print(force=False)

                elif k == "out_time" and out_time_s is None:
                    # Fallback timing field, like "00:00:12.345678".
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
                    # When ffmpeg ends, it emits progress=end.
                    if v == "end" and tty:
                        maybe_print(force=True)

            else:
                # Non "-progress" diagnostic lines can still be useful.
                t = line.strip()
                if not t:
                    continue
                low = t.lower()
                # Print only interesting bits; avoid spamming normal frame logs.
                if "error" in low or "invalid" in low or "warning" in low or "fontconfig" in low:
                    print("[ffmpeg]", t, flush=True)

        else:
            # No new line from stderr; check whether process ended.
            if p.poll() is not None:
                break

            # If ffmpeg goes quiet, emit a heartbeat so the user knows it's alive.
            if time.time() - last_output >= HEARTBEAT_SEC:
                maybe_print(force=True)
                print(f"...still running (no output for {HEARTBEAT_SEC}s)...", flush=True)
                last_output = time.time()
            time.sleep(0.2)

    rc = p.wait()

    # Final status update
    if saw_any_progress:
        if out_time_s is not None and duration_sec and duration_sec > 0:
            file_pct = max(0.0, min(100.0, (out_time_s / duration_sec) * 100.0))
        maybe_print(force=True)

    if check and rc != 0:
        raise subprocess.CalledProcessError(rc, cmd)
    return rc


def ffprobe_has_audio(path: Path) -> bool:
    # Return True if the media file contains at least one audio stream.
    # We use ffprobe to detect this reliably.

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
    # Return the duration (seconds) of the file using ffprobe.
    # If ffprobe fails or output is not numeric, return None.

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
    # Parse 'YYYY-MM-DD' and 'HH-MM-SS' into a datetime.
    return datetime.strptime(f"{date_s}_{time_s}", "%Y-%m-%d_%H-%M-%S")


def ensure_dir(p: Path) -> None:
    # Create directory if it doesn't exist.
    p.mkdir(parents=True, exist_ok=True)


def write_concat_list(file_paths: list[Path], out_txt: Path) -> None:
    # Write a concat-demuxer list file:
    #     file 'C:\path\to\seg1.mp4'
    #     file 'C:\path\to\seg2.mp4'
    # We use absolute paths and escape single quotes for ffmpeg's parser.

    lines: list[str] = []
    for p in file_paths:
        s = str(p.resolve()).replace("'", r"'\''")
        lines.append(f"file '{s}'")
    out_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")


def find_date_folders(root: Path) -> dict[str, list[Path]]:
    # Scan ROOT_DIR for folders matching FOLDER_RE and group them by date.
    # Returns:
    #     { "YYYY-MM-DD": [Path(folder1), Path(folder2), ...] }
    # Each date list is sorted by datetime.

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

    # Sort each date's folders in chronological order
    out: dict[str, list[Path]] = {}
    for date_s, items in grouped.items():
        items.sort(key=lambda x: x[0])
        out[date_s] = [p for _, p in items]
    return out


def find_camera_files(folder: Path) -> dict[str, Path]:
    # Find all camera files in a segment folder and return:
    #     { "front": Path(...), "back": Path(...), "left": Path(...), "right": Path(...) }
    # Missing cameras are simply absent from the dict.

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


def pick_audio_camera_for_segment(cams: dict[str, Path]) -> str | None:
    # Choose which camera's audio to use.
    # We prefer a stable priority order and select the first file that actually has audio.

    for cam in ["front", "back", "left", "right"]:
        p = cams.get(cam)
        if p and ffprobe_has_audio(p):
            return cam
    return None


def print_ffmpeg_version() -> None:
    # Print the first line of `ffmpeg -version` (useful in public logs and bug reports).
    try:
        p = subprocess.run([FFMPEG, "-version"], capture_output=True, text=True, encoding="utf-8", errors="replace")
        if p.stdout:
            print("\n=== ffmpeg -version ===")
            print(p.stdout.splitlines()[0])
            print("======================\n")
    except Exception as e:
        print(f"Could not run ffmpeg -version: {e}")


def pick_windows_fontfile() -> str | None:
    # Return an existing .ttf font path on Windows (forward slashes recommended for ffmpeg drawtext).
    # We try user-provided TIMESTAMP_FONTFILE first, then common built-in fonts.

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
    # Build a drawtext overlay that displays:
    #
    #     YYYY-MM-DD HH:MM:SSxFF
    #
    # Where FF is the frame number within the second (mod OUTPUT_FPS).
    # The overlay time is derived from the folder name and shifted by TIMESTAMP_SHIFT_HOURS.
    #
    # Implementation detail:
    # - Some ffmpeg 4.4 builds are picky about 02d padding inside eif.
    # - We print each 2-digit number by concatenating tens digit and ones digit.
    #
    # Returns:
    #     filter string or None if DRAW_TIMESTAMP is False or folder doesn't match.

    if not DRAW_TIMESTAMP:
        return None

    m = FOLDER_RE.match(folder_name)
    if not m:
        return None

    dt_src = parse_dt(m.group("date"), m.group("time"))
    dt_adj = dt_src + timedelta(hours=TIMESTAMP_SHIFT_HOURS)

    # Date is already zero-padded by strftime
    date_text = dt_adj.strftime("%Y-%m-%d")

    # Seconds since midnight for adjusted start time (used as base offset)
    base_sec = dt_adj.hour * 3600 + dt_adj.minute * 60 + dt_adj.second

    # Prefer a real fontfile (best cross-platform reliability).
    fontfile = pick_windows_fontfile()
    if fontfile:
        # Important: Windows drive letter needs escaping of ":" for ffmpeg option parsing.
        ff_font = fontfile.replace("\\", "/").replace(":", r"\:")
        font_opt = f"fontfile='{ff_font}':"
    else:
        font_opt = f"font='{TIMESTAMP_FONT_FALLBACK_NAME}':"

    box = "1" if TIMESTAMP_BOX else "0"
    boxcolor = f"black@{TIMESTAMP_BOX_OPACITY}" if TIMESTAMP_BOX else "black@0"
    y = f"h-text_h-{TIMESTAMP_PADDING_Y}"

    # Expressions for H/M/S/F from current time 't' and frame number 'n':
    H = f"trunc((t+{base_sec})/3600)"
    M = f"mod(trunc((t+{base_sec})/60)\\,60)"
    S = f"mod(trunc(t+{base_sec})\\,60)"
    F = f"mod(n\\,{OUTPUT_FPS})"

    # Two-digit printing: tens + ones (each printed as decimal digit).
    def two_digits(expr: str) -> str:
        return (
            f"%{{eif\\:trunc(({expr})/10)\\:d}}"
            f"%{{eif\\:mod(({expr})\\,10)\\:d}}"
        )

    # Note: inside drawtext text, ":" must be escaped as "\:"
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
# Segment build
# =============================================================================

def build_segment_4k(
        folder: Path,
        cams: dict[str, Path],
        out_seg: Path,
        date_s: str,
        date_idx: int,
        date_total: int,
        seg_idx: int,
        seg_total: int,
) -> None:

    # Build one merged 4K segment from 4 camera files.
    #
    # Video pipeline:
    #   - Force constant frame rate via setpts to OUTPUT_FPS
    #   - Pad each input to 1920x1080 (centered)
    #   - hstack top row (front+back), hstack bottom row (left+right)
    #   - vstack rows => 3840x2160
    #   - Optional timestamp overlay
    #   - Encode with libx264
    #
    # Audio pipeline:
    #   - If at least one camera has audio: map that stream
    #   - Else: generate silent stereo audio of segment duration

    audio_cam = pick_audio_camera_for_segment(cams)

    # Duration for progress estimation (not strictly required, but nice UX).
    # We take the minimum duration across cameras to avoid percent>100% if one stream is longer.
    durations: list[float] = []
    for cam in ["front", "back", "left", "right"]:
        d = ffprobe_duration_sec(cams[cam])
        if d and d > 0:
            durations.append(d)
    seg_dur = min(durations) if durations else 60.0
    duration_for_pct = seg_dur

    overlay = build_overlay_filter_dynamic(folder.name)

    cmd: list[str] = [
        FFMPEG, "-y",
        "-hide_banner",
        "-loglevel", FFMPEG_LOGLEVEL,
        "-stats",
        "-progress", "pipe:2",
        "-fflags", "+genpts",
        "-avoid_negative_ts", "make_zero",
        "-i", str(cams["front"]),
        "-i", str(cams["back"]),
        "-i", str(cams["left"]),
        "-i", str(cams["right"]),
    ]

    # If there is no audio anywhere, add a silent audio source as input #4.
    if audio_cam is None:
        cmd += ["-f", "lavfi", "-t", f"{seg_dur:.3f}", "-i", "anullsrc=r=48000:cl=stereo"]

    # Filter graph:
    # - setpts ensures timebase matches our intended CFR (helps with some DVR quirks)
    # - pad ensures consistent 1920x1080 tiles without scaling (black bars if needed)
    # - setsar=1 normalizes sample aspect ratio
    fg = [
        f"[0:v]setpts=N/({OUTPUT_FPS}*TB),pad={TILE_W}:{TILE_H}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1[v_f]",
        f"[1:v]setpts=N/({OUTPUT_FPS}*TB),pad={TILE_W}:{TILE_H}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1[v_b]",
        f"[2:v]setpts=N/({OUTPUT_FPS}*TB),pad={TILE_W}:{TILE_H}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1[v_l]",
        f"[3:v]setpts=N/({OUTPUT_FPS}*TB),pad={TILE_W}:{TILE_H}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1[v_r]",
        "[v_f][v_b]hstack=inputs=2[top]",
        "[v_l][v_r]hstack=inputs=2[bot]",
        "[top][bot]vstack=inputs=2[vstacked]",
    ]

    if overlay:
        fg.append(f"[vstacked]{overlay}[vout]")
    else:
        # 'copy' here is a filter (not stream copy) to give a named output label.
        fg.append("[vstacked]copy[vout]")

    filtergraph = ";".join(fg)

    cmd += ["-filter_complex", filtergraph, "-map", "[vout]"]

    # Map audio:
    # - If audio_cam exists, map its first audio stream (optional with '?')
    # - Else map the synthetic silent audio (input #4)
    if audio_cam is not None:
        a_map = {"front": "0", "back": "1", "left": "2", "right": "3"}[audio_cam]
        cmd += ["-map", f"{a_map}:a:0?"]
    else:
        cmd += ["-map", "4:a:0"]

    # Enforce output FPS and stop at the shortest stream (prevents trailing black video or long audio).
    cmd += ["-r", str(OUTPUT_FPS)]
    cmd += ["-shortest"]

    # Encoding settings:
    # - libx264 CRF controls quality/bitrate tradeoff (lower CRF = higher quality)
    # - pix_fmt yuv420p maximizes compatibility with players
    cmd += [
        "-c:v", "libx264",
        "-preset", PRESET,
        "-crf", str(CRF),
        "-pix_fmt", "yuv420p",
        "-profile:v", "high",
        "-level:v", "5.1",
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
        # Per-date work directory keeps intermediate segments organized.
        work_dir = OUT_DIR / f"_work_{date_s}"
        ensure_dir(work_dir)

        if VERBOSE:
            print(
                f"\nProcessing date {date_s} [{date_idx}/{date_total} {fmt_pct(pct(date_idx, date_total))}] "
                f"({len(folders)} folders found)",
                flush=True,
            )

        # Identify “complete” segments (all 4 cameras present).
        complete: list[tuple[Path, dict[str, Path]]] = []
        skipped = 0

        for folder in folders:
            cams = find_camera_files(folder)
            if not all(k in cams for k in ["front", "back", "left", "right"]):
                skipped += 1
                continue
            complete.append((folder, cams))

        seg_total = len(complete)
        print(f"  segments ready: {seg_total}, skipped incomplete: {skipped}", flush=True)

        if seg_total == 0:
            print(f"Skipping {date_s}: no complete segments found.", flush=True)
            continue

        segment_files: list[Path] = []
        built = 0
        reused = 0

        # Build or reuse each segment
        for seg_idx, (folder, cams) in enumerate(complete, start=1):
            out_seg = work_dir / f"seg_{folder.name}.mp4"

            if SKIP_EXISTING_SEGMENTS and out_seg.exists() and out_seg.stat().st_size > 0:
                # Still print a “progress-looking” message so the run output is consistent.
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

            build_segment_4k(
                folder=folder,
                cams=cams,
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

        # Concatenate segments (stream copy) into a single file for the date.
        concat_list = work_dir / f"concat_segments_{date_s}.txt"
        write_concat_list(segment_files, concat_list)

        out_mp4 = OUT_DIR / f"{date_s}_4CAM_4K.mp4"

        # Estimate total duration for progress % during concat (optional).
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
