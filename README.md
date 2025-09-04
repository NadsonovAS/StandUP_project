# StandUP Project

A set of tools for processing and analyzing YouTube content in the genre of comedy shows.

The project automates the entire process: from downloading audio from playlists and transcribing them using parakeet-mlx to deep content analysis. Utilizing the native capabilities of Apple Silicon and the SoundAnalysis framework, moments of laughter in audio tracks are detected. In parallel, using the Gemini CLI, performance topics are extracted and classified, which further allows for the creation of detailed metrics and dashboards for analyzing topic popularity, audience reaction, and other key aspects of performances.

Works exclusively on macOS with Apple Silicon.

## Implemented

![alt text](image.png)

### Storage

*   **Docker**: Containerization of services (PostgreSQL, MinIO).
*   **PostgreSQL**: Relational database for storing metadata and processing results.
*   **MinIO**: S3-compatible object storage for audio files.

### Extract and transfrom
*   **yt-dlp**: Downloading audio and metadata from YouTube.
*   **parakeet-mlx**: Audio transcription on Apple Silicon.
*   **SoundAnalysis**: Apple framework for sound analysis (used for laughter detection).
*   **Gemini CLI**: Large language model for text analysis.

## Planned Features

*   Normalized schema and data migration from raw layer
*   DBT
*   Superset

## Planned Metrics for Dashboard:


Analysis of selected show:
*   Top 10 popular videos by (choice): views, likes, number of comments.
*   Most frequently occurring topics.
*   Top 10 Topics by reaction (aggregation of laughter detection).
*   Top 10 videos by reaction (title, total number of laughter detections, total duration of laughter, average time between laughs).
*   Advertising (frequency of appearance, try to extract brands).
*   Amount of profanity in video/topic.
*   ...and more.

Switches: show, topic, year, comedians...

## Project Structure

```txt
StandUP_project
├── .env                        # Environment variables file
├── .gitignore                  # File to exclude files from Git
├── docker-compose.yml          # Configuration for running services in Docker
├── initdb/
│   └── init_schema.sql         # SQL script for initializing the DB schema
├── pyproject.toml              # Project dependencies and metadata definition
├── README.md                   # Project documentation
├── data/                       # Directory for storing temporary audio files
└── src/
    ├── config.py               # Project configuration (paths, keys, settings)
    ├── database.py             # Functions for working with PostgreSQL database
    ├── llm.py                  # Functions for interacting with Gemini API
    ├── main.py                 # Main script for running the pipeline
    ├── models.py               # Pydantic models for data validation
    ├── sound_classifier        # Executable Swift file for sound analysis
    ├── sound_classifier.py     # Module for running the Swift sound analysis script
    ├── sound_classifier.swift  # Swift source code for sound analysis
    ├── transcribe.py           # Module for audio transcription
    ├── utils.py                # Helper functions
    └── youtube_downloader.py   # Module for downloading data from YouTube
```

## Setup and Launch

1.  **Clone the repository:**
    ```bash
    git clone <repository URL>
    cd StandUP_project
    ```

2.  **Create and configure the `.env` file:**
    Create a `.env` file in the project root and fill it according to the following template:
    ```env
    # MinIO Configuration
    MINIO_ROOT_USER=standup_project
    MINIO_ROOT_PASSWORD=standup_project
    MINIO_DOMAIN=localhost:9000

    # PostgreSQL Configuration
    POSTGRES_DB=standup_project
    POSTGRES_USER=standup_project
    POSTGRES_PASSWORD=standup_project
    POSTGRES_HOST=localhost
    POSTGRES_PORT=5432
    ```

3.  **Start services with Docker Compose:**
    ```bash
    docker-compose up -d
    ```
    This command will start containers with PostgreSQL and MinIO.

4.  **Run the pipeline:**
    Pass the YouTube playlist URL as a command-line argument (the URL must contain "...&list=..."):
    ```bash
    uv run ./src/main.py "https://www.youtube.com/watch?v=MaVc3dqiEI4&list=PLcQngyvNgfmLi9eyV9reNMqu-pbdKErKr"
    ```