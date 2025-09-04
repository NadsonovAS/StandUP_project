from parakeet_mlx import from_pretrained

from utils import try_except_with_log


@try_except_with_log("Starting audio transcription")
def transcribe_audio(audio_path):
    """
    Transcribes an audio file using parakeet_mlx.

    Args:
        audio_path (str): The path to the audio file.

    Returns:
        dict: A dictionary containing the transcribed text with timestamps.
    """
    model = from_pretrained("mlx-community/parakeet-tdt-0.6b-v3")
    result = model.transcribe(
        audio_path,
        chunk_duration=30.0,
        overlap_duration=3.0,
    )

    dict_for_db = {}

    for num, sentence in enumerate(result.sentences):
        start_time = round(sentence.start, 2)
        end_time = round(sentence.end, 2)
        text = sentence.text

        dict_for_db[num] = {"start": start_time, "end": end_time, "text": text}

    return dict_for_db
