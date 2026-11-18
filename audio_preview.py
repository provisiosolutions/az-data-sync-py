"""Zero-dependency PCM WAV waveform extraction and cross-platform audio playback."""

from __future__ import annotations

import array
import math
import shutil
import subprocess
import sys
import time
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from rich.text import Text

_BLOCKS = "▁▂▃▄▅▆▇█"
_NOISE_FLOOR_PEAK = 0.015
_MIN_DBFS = -96.0
_MAX_WAVEFORM_CACHE = 512

_WAVEFORM_CACHE: dict[tuple[Path, int], tuple[int, int, "WavWaveform"]] = {}


@dataclass(frozen=True)
class WavWaveform:
    """Compact amplitude envelope and audio header metrics for a PCM WAV clip."""

    bins: tuple[float, ...]
    raw_peak: float
    peak_dbfs: float
    rms_dbfs: float
    duration_s: float
    sample_rate: int
    channels: int
    sample_width_bits: int


def _to_dbfs(linear: float) -> float:
    if linear <= 0.0:
        return _MIN_DBFS
    return max(_MIN_DBFS, 20.0 * math.log10(linear))


def _decode_pcm_samples(raw_bytes: bytes, sampwidth: int) -> tuple[array.array | list[int], float] | None:
    """Decode little-endian PCM WAV bytes into signed sample sequences and full-scale divisor."""
    if sampwidth == 2:
        aligned = len(raw_bytes) - (len(raw_bytes) % 2)
        if aligned <= 0:
            return None
        samples = array.array("h")
        samples.frombytes(raw_bytes[:aligned])
        if sys.byteorder != "little":
            samples.byteswap()
        return samples, 32768.0

    if sampwidth == 1:
        return [b - 128 for b in raw_bytes], 128.0

    if sampwidth == 3:
        aligned = len(raw_bytes) - (len(raw_bytes) % 3)
        if aligned <= 0:
            return None
        samples_24 = [
            int.from_bytes(raw_bytes[i : i + 3], "little", signed=True)
            for i in range(0, aligned, 3)
        ]
        return samples_24, 8388608.0

    if sampwidth == 4:
        aligned = len(raw_bytes) - (len(raw_bytes) % 4)
        if aligned <= 0:
            return None
        samples_32 = array.array("i")
        samples_32.frombytes(raw_bytes[:aligned])
        if sys.byteorder != "little":
            samples_32.byteswap()
        return samples_32, 2147483648.0

    return None


def extract_wav_waveform(wav_path: Path, num_bins: int = 56) -> WavWaveform | None:
    """Extract a cached, normalized peak-amplitude waveform envelope from a PCM WAV file."""
    if num_bins <= 0 or not wav_path.is_file():
        return None

    try:
        st = wav_path.stat()
    except OSError:
        return None

    cache_key = (wav_path.resolve(), num_bins)
    cached = _WAVEFORM_CACHE.get(cache_key)
    if cached and cached[0] == st.st_size and cached[1] == st.st_mtime_ns:
        return cached[2]

    try:
        with wave.open(str(wav_path), "rb") as wf:
            nchannels = wf.getnchannels()
            sampwidth = wf.getsampwidth()
            framerate = wf.getframerate()
            nframes = wf.getnframes()
            if nchannels <= 0 or framerate <= 0 or nframes <= 0:
                return None
            raw_bytes = wf.readframes(nframes)
    except (wave.Error, EOFError, OSError):
        return None

    decoded = _decode_pcm_samples(raw_bytes, sampwidth)
    if decoded is None:
        return None

    samples, full_scale = decoded
    total_samples = len(samples)
    total_frames = total_samples // nchannels
    if total_frames <= 0:
        return None

    raw_bins: list[float] = []
    for idx in range(num_bins):
        frame_start = (idx * total_frames) // num_bins
        frame_end = max(frame_start + 1, ((idx + 1) * total_frames) // num_bins)
        s_start = frame_start * nchannels
        s_end = min(total_samples, frame_end * nchannels)
        if s_start >= s_end:
            raw_bins.append(0.0)
            continue
        chunk = samples[s_start:s_end]
        hi = max(chunk)
        lo = min(chunk)
        peak_val = max(abs(hi), abs(lo)) / full_scale
        raw_bins.append(peak_val)

    raw_peak = max(raw_bins, default=0.0)
    stride = max(1, total_samples // 2048)
    sq_sum = 0.0
    rms_count = 0
    for idx in range(0, total_samples, stride):
        norm = samples[idx] / full_scale
        sq_sum += norm * norm
        rms_count += 1
    rms_val = math.sqrt(sq_sum / rms_count) if rms_count > 0 else 0.0

    denom = max(raw_peak, _NOISE_FLOOR_PEAK)
    norm_bins = tuple(min(1.0, b / denom) for b in raw_bins)
    duration_s = round(nframes / framerate, 6)

    wf_result = WavWaveform(
        bins=norm_bins,
        raw_peak=raw_peak,
        peak_dbfs=_to_dbfs(raw_peak),
        rms_dbfs=_to_dbfs(rms_val),
        duration_s=duration_s,
        sample_rate=framerate,
        channels=nchannels,
        sample_width_bits=sampwidth * 8,
    )

    if len(_WAVEFORM_CACHE) >= _MAX_WAVEFORM_CACHE:
        _WAVEFORM_CACHE.pop(next(iter(_WAVEFORM_CACHE)), None)
    _WAVEFORM_CACHE[cache_key] = (st.st_size, st.st_mtime_ns, wf_result)
    return wf_result


def render_waveform_line(wf: WavWaveform, progress: float | None = None) -> Text:
    """Render a Unicode speech waveform with an optional animated playback playhead."""
    out = Text()
    out.append("▐", style="dim")
    bin_count = len(wf.bins)
    playhead = -1
    if progress is not None and bin_count > 0:
        playhead = min(bin_count - 1, max(0, int(progress * bin_count)))

    for idx, val in enumerate(wf.bins):
        char_idx = min(len(_BLOCKS) - 1, max(0, int(round(val * (len(_BLOCKS) - 1)))))
        ch = _BLOCKS[char_idx]
        if playhead >= 0:
            if idx <= playhead:
                style = "bold bright_yellow"
            elif val >= 0.10:
                style = "cyan"
            else:
                style = "dim"
        else:
            if val < 0.10:
                style = "dim"
            elif val < 0.60:
                style = "cyan"
            else:
                style = "bold green"
        out.append(ch, style=style)

    out.append("▌", style="dim")
    return out


def detect_audio_backend() -> tuple[str, list[str]] | None:
    """Detect an available non-blocking local audio player for the current OS."""
    if sys.platform == "darwin":
        for name, cmd in (
            ("afplay", ["afplay"]),
            ("ffplay", ["ffplay", "-nodisp", "-autoexit", "-loglevel", "quiet"]),
        ):
            resolved = shutil.which(cmd[0])
            if resolved:
                return name, [resolved, *cmd[1:]]
        return None

    if sys.platform == "win32":
        return "winsound", []

    for name, cmd in (
        ("pw-play", ["pw-play"]),
        ("paplay", ["paplay"]),
        ("aplay", ["aplay", "-q"]),
        ("ffplay", ["ffplay", "-nodisp", "-autoexit", "-loglevel", "quiet"]),
    ):
        resolved = shutil.which(cmd[0])
        if resolved:
            return name, [resolved, *cmd[1:]]
    return None


class AudioPlayer:
    """Non-blocking single-clip WAV player with clean stop/replacement lifecycle."""

    def __init__(self, runner: Callable[[Path], object] | None = None) -> None:
        self._runner = runner
        self._proc = None
        self._active_path: Path | None = None
        self._started_at: float = 0.0
        self._duration_s: float = 0.0
        self._using_winsound: bool = False

    def play(self, wav_path: Path, duration_s: float = 0.0) -> tuple[bool, str]:
        """Start playing wav_path non-blockingly, stopping any previous clip first."""
        self.stop()
        if not wav_path.is_file():
            return False, f"File not found: {wav_path}"

        if self._runner is not None:
            try:
                self._proc = self._runner(wav_path)
            except Exception as exc:
                return False, f"Playback error: {exc}"
            self._active_path = wav_path
            self._started_at = time.monotonic()
            self._duration_s = max(duration_s, 0.15)
            return True, "mock"

        backend = detect_audio_backend()
        if backend is None:
            return (
                False,
                "No local audio player found (install afplay, pw-play, paplay, aplay, or ffplay).",
            )

        backend_name, cmd = backend
        if backend_name == "winsound":
            try:
                import winsound

                winsound.PlaySound(
                    str(wav_path),
                    winsound.SND_FILENAME | winsound.SND_ASYNC,
                )
            except Exception as exc:
                return False, f"Windows playback failed: {exc}"
            self._using_winsound = True
            self._active_path = wav_path
            self._started_at = time.monotonic()
            self._duration_s = max(duration_s, 0.15)
            return True, "winsound"

        try:
            self._proc = subprocess.Popen(
                [*cmd, str(wav_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError as exc:
            return False, f"Failed to start {backend_name}: {exc}"

        self._active_path = wav_path
        self._started_at = time.monotonic()
        self._duration_s = max(duration_s, 0.15)
        return True, backend_name

    def stop(self) -> bool:
        """Stop active playback immediately and return whether a clip was playing."""
        was_active = self.is_playing()
        proc = self._proc
        using_winsound = self._using_winsound
        self._proc = None
        self._active_path = None
        self._using_winsound = False

        if using_winsound:
            try:
                import winsound

                winsound.PlaySound(None, winsound.SND_PURGE)
            except Exception:
                pass

        if proc is not None:
            try:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=0.15)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

        return was_active

    def is_playing(self) -> bool:
        """Return True while the launched audio backend is still active."""
        if self._proc is not None:
            if self._proc.poll() is None:
                return True
            self._proc = None
            self._active_path = None
            return False

        if self._using_winsound:
            if (time.monotonic() - self._started_at) < self._duration_s:
                return True
            self._using_winsound = False
            self._active_path = None
            return False

        return False

    def progress(self) -> float | None:
        """Return elapsed playback fraction in [0.0, 1.0] when playing, else None."""
        if not self.is_playing() or self._duration_s <= 0.0:
            return None
        elapsed = time.monotonic() - self._started_at
        return min(1.0, max(0.0, elapsed / self._duration_s))

    @property
    def active_path(self) -> Path | None:
        """Return currently playing WAV path or None if idle."""
        return self._active_path if self.is_playing() else None
