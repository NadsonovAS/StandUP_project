import json
import subprocess
from typing import Callable, Mapping, Sequence

import numpy as np

from config import Settings, get_settings
from utils import try_except_with_log

EventDict = dict[str, float | int]


def build_classifier_command(audio_path: str, settings: Settings) -> list[str]:
    return [
        "./src/sound_classifier",
        str(audio_path),
        str(settings.WINDOW_DURATION_SECONDS),
        str(settings.PREFERRED_TIMESCALE),
        str(settings.CONFIDENCE_THRESHOLD),
        str(settings.OVERLAP_FACTOR),
    ]


def run_command_default(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )


class SoundClassifierClient:
    """Encapsulates invocation of the Swift laughter detector binary."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        runner: Callable[
            [Sequence[str]], subprocess.CompletedProcess[str]
        ] = run_command_default,
        command_builder: Callable[
            [str, Settings], list[str]
        ] = build_classifier_command,
    ) -> None:
        self._settings = settings or get_settings()
        self._runner = runner
        self._command_builder = command_builder
        self._events_avg_confidence_threshold = (
            self._settings.LAUGH_EVENT_AVG_CONFIDENCE_THRESHOLD
        )
        self._events_min_duration = self._settings.LAUGH_EVENT_MIN_DURATION_SECONDS
        self._events_max_gap = self._settings.LAUGH_EVENT_MAX_GAP_SECONDS

    @try_except_with_log("Starting laughter detection")
    def classify_audio(self, audio_path: str) -> dict[str, float]:
        command = self._command_builder(audio_path, self._settings)
        completed_process = self._runner(command)
        payload = json.loads(completed_process.stdout)
        return {str(key): float(value) for key, value in payload.items()}

    def _to_sorted_arrays(
        self, raw: Mapping[str, float] | None
    ) -> tuple[np.ndarray, np.ndarray]:
        if not raw:
            return np.array([], dtype=float), np.array([], dtype=float)

        pairs = np.array(
            [
                (float(timestamp), float(confidence))
                for timestamp, confidence in raw.items()
            ],
            dtype=float,
        )
        times = pairs[:, 0]
        confidences = pairs[:, 1]
        order = np.argsort(times)
        return times[order], confidences[order]

    @try_except_with_log("Starting laughter event analyze")
    def analyze_laugh_events(self, raw: Mapping[str, float] | None) -> list[EventDict]:
        times, confidences = self._to_sorted_arrays(raw)
        if times.size == 0:
            return []

        # Cluster by time gaps using all timestamps — do NOT prefilter individual points by confidence.
        split_points = np.where(np.diff(times) > self._events_max_gap)[0] + 1
        cluster_starts = np.concatenate(([0], split_points))
        cluster_ends = np.concatenate((split_points, [times.size]))

        events: list[EventDict] = []
        for start_idx, end_idx in zip(cluster_starts, cluster_ends):
            cluster_times = times[start_idx:end_idx]
            cluster_conf = confidences[start_idx:end_idx]

            start = float(cluster_times[0])
            end = float(cluster_times[-1])
            duration = end - start
            points = int(end_idx - start_idx)

            avg_confidence = float(cluster_conf.mean()) if cluster_conf.size else 0.0
            max_confidence = float(cluster_conf.max()) if cluster_conf.size else 0.0

            # Apply the confidence threshold only to the cluster's average confidence.
            if (
                duration >= self._events_min_duration
                and avg_confidence >= self._events_avg_confidence_threshold
            ):
                events.append(
                    {
                        "start": start,
                        "end": end,
                        "duration": duration,
                        "points": points,
                        "avg_confidence": avg_confidence,
                        "max_confidence": max_confidence,
                    }
                )

        return events

    def serialize_events(self, events: list[EventDict]) -> list[EventDict]:
        serialized: list[EventDict] = []
        for index, event in enumerate(events, start=1):
            serialized.append(
                {
                    "sequence": index,
                    "start_seconds": round(float(event["start"]), 2),
                    "end_seconds": round(float(event["end"]), 2),
                    "duration_seconds": round(float(event["duration"]), 2),
                    "points": int(event["points"]),
                    "avg_confidence": round(float(event["avg_confidence"]), 2),
                    "max_confidence": round(float(event["max_confidence"]), 2),
                }
            )
        return serialized

    def build_laugh_events_payload(
        self, raw: Mapping[str, float] | None
    ) -> dict[str, list[EventDict]]:
        events = self.analyze_laugh_events(raw)
        return {"events": self.serialize_events(events)}
