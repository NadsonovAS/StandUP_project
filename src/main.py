import argparse
import logging

from pydantic import ValidationError

from data_pipeliine import run_pipeline

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Process a new YouTube playlist or resume unfinished videos from the"
            " database"
        )
    )
    parser.add_argument(
        "--new_playlist",
        dest="new_playlist",
        help="URL of the new playlist to ingest",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the video processing pipeline."""
    args = parse_args()
    try:
        run_pipeline(args.new_playlist)
    except ValidationError as exc:
        logging.error("URL validation error: %s", exc)
    except KeyboardInterrupt:
        logging.warning("Operation interrupted by user (KeyboardInterrupt)")


if __name__ == "__main__":
    main()
