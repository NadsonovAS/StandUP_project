# Repository Guidelines

## Project Structure & Module Organization
- `src/` hosts the ingestion pipeline: `main.py` orchestrates, `youtube_downloader.py` and `database.py` handle persistence, `transcribe.py` and `llm.py` wrap external services, and the laughter detector ships as the compiled `sound_classifier` plus `sound_classifier.swift`. Config defaults sit in `config.py`; shared models in `models.py`.
- `standup_project/` is the dbt workspace containing models, macros, snapshots, and seeds; compiled artefacts land under `standup_project/target/`.
- Infrastructure assets include `docker-compose.yml` for PostgreSQL + MinIO, `initdb/` bootstrap SQL, and runtime caches under `data/` and `logs/` (gitignored).

## Build and Development Commands
- Install toolchain with `uv sync`.
- Start services via `docker-compose up -d`; stop with `docker-compose down` when finished.
- Process a playlist using `uv run src/main.py "<playlist-url-with-list>"`.
- Refresh analytics through `uv run dbt build`.
- If you modify the Swift detector, rebuild it with `xcrun swiftc -target arm64-apple-macos13 -framework SoundAnalysis -framework AVFoundation src/sound_classifier.swift -o src/sound_classifier`.

## Coding Style & Naming Conventions
- Target Python 3.13 and PEP 8: four-space indents, snake_case identifiers, PascalCase for Pydantic models, and explicit type hints as in `src/main.py`.
- Centralise configuration through `config.Settings`; resist direct `os.environ` reads elsewhere.
- Reuse the shared `logging` setup and keep long-running actions behind small, testable helpers.

## Commit & Pull Request Guidelines
- Use short imperative commit subjects (`Integrate dbt pipeline trigger`, `Handle empty normalized titles`) capped near 72 characters; extend explanations in the body if behaviour changes.
- PRs should describe intent, list verification commands (`uv run dbt build`), link issues, and call out schema or config updates reviewers must apply.
- Attach artefact samples only when JSON payloads or dbt outputs change; otherwise reference the affected tables or models.

## Environment & Configuration Tips
- Keep secrets in `.env`, mirror schema updates between `initdb/` and dbt models, and avoid committing generated data.
- Align MinIO bucket/path overrides with `config.Settings`; update Python sources and dbt seeds together if storage layout shifts.
