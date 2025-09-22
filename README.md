# StandUP Project

StandUP automates the ingestion and analysis of YouTube comedy shows on Apple Silicon. The pipeline downloads playlists, transcribes audio, detects laughter, classifies topics with Gemini, and prepares analytics-ready tables backed by PostgreSQL, MinIO, and dbt.

![Pipeline overview](image.png)

## Key Capabilities
- Download playlist metadata and audio with `yt-dlp`, caching results in MinIO.
- Transcribe tracks locally via `parakeet-mlx` while detecting laughter using the SoundAnalysis Swift binary.
- Summarise and classify transcripts through the Gemini CLI, persisting structured output to PostgreSQL.
- Build downstream analytics models with dbt for dashboards and reporting.

## System Requirements
- macOS 14+ on Apple Silicon (SoundAnalysis and parakeet-mlx depend on it).
- Docker Desktop for PostgreSQL and MinIO containers.
- Python 3.13 managed by [`uv`](https://github.com/astral-sh/uv) (installs project dependencies).
- Safari signed in to YouTube; `yt-dlp` pulls cookies via the Safari browser integration.
- `ffmpeg` available on the host for audio post-processing (installable via `brew install ffmpeg`).

## Quick Start
1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd StandUP_project
   ```
2. **Install Python dependencies**
   ```bash
   uv sync
   ```
3. **Create `.env` in the project root**
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
   ```
   The `MINIO_DOMAIN` value should match how you access the MinIO console locally.
4. **Start infrastructure**
   ```bash
   docker-compose up -d
   ```
   This launches PostgreSQL, MinIO, and a helper container that bootstraps the `standup-project` bucket.
5. **Run the ingestion pipeline**
   ```bash
   uv run src/main.py "https://www.youtube.com/watch?v=MaVc3dqiEI4&list=PLcQngyvNgfmLi9eyV9reNMqu-pbdKErKr"
   ```
   Use any playlist URL that contains the `list=` parameter. The script processes each video, updates database status flags, and leaves cached audio in `data/`.

## Project Layout
```txt
StandUP_project
├── src/                     # Python pipeline modules and Swift laughter detector binary
│   ├── main.py              # Entry point orchestrating download → transcription → analysis
│   ├── youtube_downloader.py# Playlist and audio extraction helpers
│   ├── transcribe.py        # parakeet-mlx transcription logic
│   ├── sound_classifier.py  # Wrapper around the Swift SoundAnalysis executable (src/sound_classifier)
│   ├── llm.py               # Gemini CLI integration for summaries and topic labels
│   ├── database.py          # PostgreSQL access helpers (psycopg)
│   ├── models.py            # Pydantic models validating playlist items and processing state
│   ├── utils.py             # Shared helpers (logging, temp cleanup)
│   └── config.py            # Centralised settings loaded from `.env`
├── standup_project/         # dbt project for transformation, tests, and seeds
├── initdb/                  # SQL initialization scripts executed by PostgreSQL container
├── data/                    # Local cache for downloaded audio (ignored by Git)
├── logs/                    # Runtime logs and scratch outputs (ignored by Git)
├── docker-compose.yml       # Infrastructure stack (PostgreSQL, MinIO, bootstrap)
├── pyproject.toml / uv.lock # Python dependency definitions managed by uv
└── AGENTS.md                # Contributor guidelines
```

## Processing Workflow
1. Playlist metadata is fetched and normalised; entries persist to PostgreSQL.
2. Audio is downloaded or retrieved from MinIO, then cached in `data/`.
3. Transcriptions (`parakeet-mlx`) and laughter segments (SoundAnalysis Swift binary) are generated.
4. Gemini summarises chapters and classifies topics, enriching the database rows.
5. Each video row transitions to `process_status="finished"` once all artifacts exist.

Monitor progress via pipeline logs or by querying PostgreSQL for `process_status` values. MinIO retains raw audio under the `standup-project/data/audio` prefix.

## Analytics with dbt
Run dbt inside the managed Python environment to build analytics models:
```bash
uv run dbt build --project-dir standup_project
```
Use `uv run dbt test --project-dir standup_project --select <model>` to validate specific transformations.

## Troubleshooting
- If downloads fail, confirm Safari is open and authenticated with YouTube; `yt-dlp` relies on its cookies.
- Remove stale audio cache via the pipeline helper: `uv run python -c "import utils; utils.remove_audio_cache()"`.
- Inspect `logs/` for run artifacts and `docker-compose logs <service>` for container diagnostics.
