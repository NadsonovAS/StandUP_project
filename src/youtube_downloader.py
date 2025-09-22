import re
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Protocol

import yt_dlp
from minio.error import S3Error

from config import Settings, get_settings
from models import ProcessVideo
from utils import try_except_with_log


class ObjectStorageClient(Protocol):
    def stat_object(self, bucket_name: str, object_name: str) -> Any:
        ...

    def fget_object(self, bucket_name: str, object_name: str, file_path: str) -> Any:
        ...

    def fput_object(self, bucket_name: str, object_name: str, file_path: str) -> Any:
        ...


def build_audio_artifacts(video_title: str, settings: Settings) -> tuple[Path, str, str]:
    """Return local path, object name, and template for audio downloads."""
    audio_filename = f"{video_title}.opus"
    local_audio_path = settings.DATA_DIR / audio_filename
    object_name = f"{settings.MINIO_AUDIO_PATH}/{audio_filename}"
    local_audio_path_template = str(settings.DATA_DIR / video_title)
    return local_audio_path, object_name, local_audio_path_template


@try_except_with_log()
def normalize_title(title: str) -> str:
    """
    Normalize a video title for safe use as a filesystem-friendly filename.

    Steps:
    - Replace all characters not alphanumeric, underscore, or Cyrillic with underscores.
    - Collapse multiple underscores into one.
    - Trim leading and trailing underscores.
    """
    normalized = re.sub(r"[^\w\dа-яА-ЯёЁ]+", "_", title)
    normalized = re.sub(r"__+", "_", normalized)
    return normalized.strip("_")


class YoutubeDownloader:
    """Wrapper around yt-dlp operations to enable dependency injection."""

    def __init__(
        self,
        settings: Settings | None = None,
        ydl_factory: Callable[[dict], yt_dlp.YoutubeDL] = yt_dlp.YoutubeDL,
    ) -> None:
        self._settings = settings or get_settings()
        self._ydl_factory = ydl_factory

    def _with_client(self, options: dict, callback: Callable[[yt_dlp.YoutubeDL], Any]) -> Any:
        with self._ydl_factory(options) as client:
            return callback(client)

    def extract_video_info(self, video_url: str) -> Dict[str, int]:
        """Extract metadata for a single YouTube video without downloading it."""

        def _extract(client: yt_dlp.YoutubeDL) -> Dict[str, int]:
            video_info = client.extract_info(video_url, download=False)
            keys_to_extract = [
                "duration",
                "like_count",
                "view_count",
                "comment_count",
                "upload_date",
            ]
            return {key: video_info[key] for key in keys_to_extract if key in video_info}

        return self._with_client(self._settings.YDL_PLAYLIST_OPTS, _extract)

    def extract_playlist_info(self, youtube_url: str) -> List[ProcessVideo]:
        """Extract playlist metadata and return validated ProcessVideo entries."""

        def _extract(client: yt_dlp.YoutubeDL) -> List[ProcessVideo]:
            all_playlist_info = client.extract_info(youtube_url)
            playlist_id = all_playlist_info.get("id")
            playlist_title = all_playlist_info.get("title")
            entries: Iterable[dict[str, Any]] = all_playlist_info.get("entries", [])

            playlist_info: List[ProcessVideo] = []
            for entry in entries:
                video_id = entry.get("id")
                video_title = entry.get("title")
                normalized_title = (
                    normalize_title(video_title) if video_title else f"video_{video_id}"
                )

                video_data = {
                    "channel_id": entry.get("channel_id"),
                    "channel_name": entry.get("channel"),
                    "playlist_id": playlist_id,
                    "playlist_title": playlist_title,
                    "video_id": video_id,
                    "video_title": normalized_title,
                    "video_url": entry.get("url"),
                }

                playlist_info.append(ProcessVideo(**video_data))
            return playlist_info

        return self._with_client(self._settings.YDL_PLAYLIST_OPTS, _extract)

    def download_audio(
        self,
        storage_client: ObjectStorageClient,
        video_url: str,
        video_title: str,
    ) -> Path:
        """Download audio, leveraging object storage for caching."""
        local_audio_path, object_name, local_audio_template = build_audio_artifacts(
            video_title, self._settings
        )

        try:
            storage_client.stat_object(self._settings.MINIO_AUDIO_BUCKET, object_name)
            if not local_audio_path.exists():
                storage_client.fget_object(
                    self._settings.MINIO_AUDIO_BUCKET, object_name, str(local_audio_path)
                )
            return local_audio_path
        except S3Error as error:
            if error.code != "NoSuchKey":
                raise

        download_opts = self._settings.YDL_DOWNLOAD_OPTS.copy()
        download_opts["outtmpl"] = local_audio_template

        def _download(client: yt_dlp.YoutubeDL) -> None:
            client.download([video_url])

        self._with_client(download_opts, _download)

        storage_client.fput_object(
            self._settings.MINIO_AUDIO_BUCKET, object_name, str(local_audio_path)
        )
        return local_audio_path
