import re
from pathlib import Path
from typing import Dict, List

import yt_dlp
from minio import Minio
from minio.error import S3Error

from config import settings
from models import ProcessVideo
from utils import try_except_with_log


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


@try_except_with_log("Downloading video metadata")
def yt_video_extract_info(video_url: str) -> Dict[str, int]:
    """
    Extracts metadata for a single YouTube video without downloading it.

    Args:
        video_url (str): The URL of the YouTube video.

    Returns:
        Dict[str, int]: A filtered dictionary with selected metadata fields.
    """
    with yt_dlp.YoutubeDL(settings.YDL_PLAYLIST_OPTS) as ydl:
        video_info = ydl.extract_info(video_url, download=False)

        keys_to_extract = [
            "duration",
            "like_count",
            "view_count",
            "comment_count",
            "upload_date",
        ]
        return {key: video_info[key] for key in keys_to_extract if key in video_info}


@try_except_with_log("Downloading playlist metadata")
def yt_playlist_extract_info(youtube_url: str) -> List[ProcessVideo]:
    """
    Extracts metadata for a YouTube playlist without downloading videos.

    Args:
        youtube_url (str): The URL of the YouTube playlist.

    Returns:
        List[ProcessVideo]: A list of validated ProcessVideo objects for each video.
    """
    with yt_dlp.YoutubeDL(settings.YDL_PLAYLIST_OPTS) as ydl:
        all_playlist_info = ydl.extract_info(youtube_url, download=False)

    playlist_id = all_playlist_info.get("id")
    playlist_title = all_playlist_info.get("title")
    entries = all_playlist_info.get("entries", [])

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

        video_obj = ProcessVideo(**video_data)
        playlist_info.append(video_obj)

    return playlist_info


@try_except_with_log("Downloading audio")
def download_audio(minio_client: Minio, video_url: str, video_title: str) -> Path:
    """
    Downloads audio from YouTube if not already present in MinIO, saves it locally,
    uploads it to MinIO, and returns the local file path.

    Args:
        minio_client (Minio): MinIO client instance.
        video_url (str): The URL of the YouTube video.
        video_title (str): The normalized title used for file naming.

    Returns:
        Path: The local path to the downloaded audio file.
    """
    audio_filename = f"{video_title}.opus"
    local_audio_path = settings.DATA_DIR / audio_filename
    object_name = f"{settings.MINIO_AUDIO_PATH}/{audio_filename}"
    local_audio_path_template = str(settings.DATA_DIR / video_title)

    # Check MinIO for existing object
    try:
        minio_client.stat_object(settings.MINIO_AUDIO_BUCKET, object_name)
        if not local_audio_path.exists():
            minio_client.fget_object(
                settings.MINIO_AUDIO_BUCKET,
                object_name,
                str(local_audio_path),
            )
        return local_audio_path
    except S3Error as e:
        if e.code != "NoSuchKey":
            raise

    # If not found in MinIO, download via yt-dlp
    ydl_opts = settings.YDL_DOWNLOAD_OPTS.copy()
    ydl_opts["outtmpl"] = local_audio_path_template

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

    # Upload new file to MinIO
    minio_client.fput_object(
        settings.MINIO_AUDIO_BUCKET, object_name, str(local_audio_path)
    )

    return local_audio_path
