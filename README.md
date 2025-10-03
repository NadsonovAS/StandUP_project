# StandUP Project

StandUP automates the ingestion and analysis of stand-up comedy playlists from YouTube. The pipeline downloads audio, transcribes routines, detects laughter events, summarises segments with Gemini, and materialises analytics-ready tables with dbt backed by PostgreSQL and MinIO storage.

![Pipeline overview](image.png)

## Highlights
- Automates YouTube playlist ingestion with `yt-dlp`, normalises metadata, and stores raw inputs in PostgreSQL.
- Caches audio artefacts in MinIO and on disk, avoiding re-downloads across pipeline runs.
- Transcribes shows locally with the Apple Silicon–optimised `parakeet-mlx` model and detects laughter via a Swift `SoundAnalysis` binary.
- Summarises chapters and classifies topics through the Gemini CLI, persisting structured JSON for downstream reporting.
- Ships a dbt project that populates core analytics tables via `uv run dbt run` after each ingestion step.

## Prerequisites
- **Hardware/OS:** Apple Silicon running macOS 14+ (required for `SoundAnalysis` and `parakeet-mlx`).
- **Python:** [`uv`](https://github.com/astral-sh/uv) with Python 3.13 toolchain installed locally.
- **Containers:** Docker Desktop (used for PostgreSQL and MinIO).
- **CLI tooling:**
  - [`yt-dlp`](https://github.com/yt-dlp/yt-dlp) (pulled automatically via `uv sync`).
  - [`ffmpeg`](https://ffmpeg.org/) on the host (`brew install ffmpeg`).
  - [Gemini CLI](https://ai.google.dev/gemini-api/docs/get-started) authenticated with an API key and available as the `gemini` executable.
- **Browser cookies:** Safari signed in to YouTube so `yt-dlp` can reuse session cookies.

## Setup
1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd StandUP_project
   ```
2. **Install Python dependencies**
   ```bash
   uv sync
   ```
3. **Create a `.env` file** (values shown are local defaults):
   ```env
   # PostgreSQL
   POSTGRES_DB=standup_project
   POSTGRES_USER=standup_project
   POSTGRES_PASSWORD=standup_project
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432

   # MinIO
   MINIO_ROOT_USER=standup_project
   MINIO_ROOT_PASSWORD=standup_project
   MINIO_DOMAIN=localhost:9000

   # Optional overrides
   # DATA_DIR=./data
   # MINIO_AUDIO_BUCKET=standup-project
   # MINIO_AUDIO_PATH=data/audio
   ```
   Ensure `MINIO_DOMAIN` matches how you access MinIO (default is `localhost:9000`).
4. **Start infrastructure**
   ```bash
   docker-compose up -d
   ```
   This launches PostgreSQL, MinIO, and a bootstrap job that creates the `standup-project` bucket with a public policy.
5. **Confirm services** (optional)
   - PostgreSQL: `psql -h localhost -U standup_project -d standup_project -c "\dt"`
   - MinIO Console: http://localhost:9001 (use credentials from `.env`).

## Running the Ingestion Pipeline
Run the end-to-end processor with any YouTube playlist URL containing a `list=` parameter:
```bash
uv run src/main.py "https://www.youtube.com/watch?v=MaVc3dqiEI4&list=PLcQngyvNgfmLi9eyV9reNMqu-pbdKErKr"
```
The pipeline will:
- Upsert playlist entries into `standup_raw.process_video`.
- Check MinIO for cached audio before downloading via `yt-dlp`.
- Transcribe speech with `parakeet-mlx` and run the Swift laughter detector (`src/sound_classifier`).
- Call Gemini twice: once for chapter summaries, once for topic classifications.
- Mark rows as `process_status = 'finished'` when all artefacts are present.

Artifacts:
- Raw audio cache: `data/` (also uploaded to `standup-project/data/audio` in MinIO).
- Structured outputs: JSON columns in `standup_raw.process_video`.
- Logs: `logs/` (ignored by Git).

You can monitor progress via pipeline logs or SQL, e.g. `SELECT video_id, process_status FROM standup_raw.process_video ORDER BY video_id;`.

## Analytics with dbt
Build analytics layers once ingestion finishes:
```bash
uv run dbt build
```
Key models include:
- `staging/stg_process_video.sql`: exposes raw JSON fields with typed columns.
- `core/*`: normalises transcripts, chapters, laughter scores, and classifications.

### Orchestrating dbt from Python
`main.py` shells out to `uv run dbt run` after each successfully processed video, so analytics tables stay in sync with new raw data. Trigger the same command manually when needed:
```python
import subprocess

subprocess.run(
    ["uv", "run", "dbt", "run", "--project-dir", "standup_project"],
    check=True,
)
```

## Repository Layout
```text
StandUP_project
├── src/
│   ├── main.py                # End-to-end playlist processor
│   ├── youtube_downloader.py  # yt-dlp wrapper with MinIO caching helpers
│   ├── transcribe.py          # Parakeet transcription wrapper
│   ├── sound_classifier.py    # Swift laughter detector client (binary lives at src/sound_classifier)
│   ├── sound_classifier.swift # Source for rebuilding the Swift binary
│   ├── llm.py                 # Gemini CLI prompts and client helpers
│   ├── database.py            # Psycopg repository for standup_raw.process_video
│   ├── models.py              # Pydantic models for pipeline entities
│   ├── utils.py               # Shared logging utilities and cache cleanup
├── standup_project/           # dbt project (models, macros, snapshots)
├── initdb/init_schema.sql     # Database bootstrap schema for raw/core layers
├── docker-compose.yml         # Local PostgreSQL and MinIO stack
├── data/                      # Local audio cache (ignored)
├── logs/                      # Execution logs and dbt state (ignored)
└── README.md
```

## Laughter Detector Binary
The prebuilt `src/sound_classifier` binary targets Apple Silicon. Rebuild it after code changes with the system Swift toolchain:
```bash
xcrun swiftc -target arm64-apple-macos13 \
  -framework SoundAnalysis -framework AVFoundation \
  src/sound_classifier.swift -o src/sound_classifier
```
Ensure you re-run `chmod +x src/sound_classifier` if needed.

## Development Workflow
- Keep configuration in `src/config.py`; prefer adding settings there instead of reading environment variables ad hoc.
- Log via the standard library `logging` module—`main.py` configures default formatting.

## Database & Storage
- `initdb/init_schema.sql` mirrors the structure expected by the pipeline and dbt models; update it alongside any schema changes.
- MinIO bucket defaults to `standup-project` with audio stored under `data/audio/<title>.opus`.
- Processed transcripts, chapters, classifications, and laughter scores are intermediate JSON blobs which dbt flattens into core tables.

## Troubleshooting
- **yt-dlp errors:** Ensure Safari is running and signed into the correct YouTube account so cookie extraction succeeds.
- **Transcription failures:** The first `parakeet-mlx` invocation downloads weights—keep the machine awake and connected; if unsupported, pass a custom loader to `ParakeetTranscriber`.
- **Gemini CLI issues:** Verify `gemini` is on `PATH` and authenticated. The pipeline retries once before giving up; check stderr in logs for JSON parsing errors.
- **Laughter detector:** The Swift binary only runs on Apple Silicon. Rebuild if you upgrade macOS frameworks or if Gatekeeper blocks execution.
- **Cleanup:** Remove stale audio caches with `uv run python -c "from utils import remove_audio_cache; remove_audio_cache()"`.

Happy shipping!
