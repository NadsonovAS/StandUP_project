from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1].parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dbt_pipeline import DbtExecutionError, DbtPipeline


@pytest.fixture()
def pipeline_tmp_dir(tmp_path: Path) -> Path:
    project_dir = tmp_path / "standup_project"
    project_dir.mkdir()
    return project_dir


def test_pipeline_runs_steps_in_order_and_updates_state(pipeline_tmp_dir: Path) -> None:
    commands: list[list[str]] = []

    def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    state_file = pipeline_tmp_dir.parent / "state.json"
    pipeline = DbtPipeline(
        project_dir=pipeline_tmp_dir,
        run_command=runner,
        state_path=state_file,
    )

    results = pipeline.run()

    assert [result.step.layer for result in results] == ["raw", "staging", "core", "mart"]
    assert commands[0][1] == "source"
    assert "path:models/staging" in commands[1]
    assert "path:models/core" in commands[2]
    assert "path:models/mart" in commands[3]
    assert state_file.exists()
    state_data = json.loads(state_file.read_text())
    assert set(state_data["layers"]) == {"raw", "staging", "core", "mart"}


def test_pipeline_passes_incremental_vars_when_state_exists(pipeline_tmp_dir: Path) -> None:
    state_file = pipeline_tmp_dir.parent / "state.json"
    state_file.write_text(json.dumps({"layers": {"staging": "2024-01-01T00:00:00+00:00"}}))

    captured: list[list[str]] = []

    def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        captured.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    pipeline = DbtPipeline(
        project_dir=pipeline_tmp_dir,
        run_command=runner,
        state_path=state_file,
    )

    pipeline.run(layers=["staging"])

    staging_command = captured[0]
    assert staging_command[1] == "run"
    vars_index = staging_command.index("--vars")
    payload = json.loads(staging_command[vars_index + 1])
    assert payload["standup_last_run"] == "2024-01-01T00:00:00+00:00"
    assert "standup_current_run" in payload


def test_pipeline_skips_last_run_for_full_refresh(pipeline_tmp_dir: Path) -> None:
    state_file = pipeline_tmp_dir.parent / "state.json"
    state_file.write_text(json.dumps({"layers": {"mart": "2024-01-01T00:00:00+00:00"}}))

    commands: list[list[str]] = []

    def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    pipeline = DbtPipeline(
        project_dir=pipeline_tmp_dir,
        run_command=runner,
        state_path=state_file,
    )

    pipeline.run(layers=["mart"], full_refresh_layers=["mart"])

    mart_command = commands[0]
    assert mart_command[1] == "run"
    assert "--full-refresh" in mart_command
    payload = json.loads(mart_command[mart_command.index("--vars") + 1])
    assert "standup_last_run" not in payload


def test_pipeline_raises_on_failure_and_preserves_previous_state(pipeline_tmp_dir: Path) -> None:
    state_file = pipeline_tmp_dir.parent / "state.json"
    commands: list[list[str]] = []

    def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        if "path:models/core" in command:
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="boom")
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    pipeline = DbtPipeline(
        project_dir=pipeline_tmp_dir,
        run_command=runner,
        state_path=state_file,
    )

    with pytest.raises(DbtExecutionError):
        pipeline.run()

    state_data = json.loads(state_file.read_text())
    assert set(state_data["layers"]) == {"raw", "staging"}
    assert commands[2][1] == "run"


def test_make_task_callables_runs_individual_layers(pipeline_tmp_dir: Path) -> None:
    state_file = pipeline_tmp_dir.parent / "state.json"
    commands: list[list[str]] = []

    def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    pipeline = DbtPipeline(
        project_dir=pipeline_tmp_dir,
        run_command=runner,
        state_path=state_file,
    )

    tasks = pipeline.make_task_callables()
    tasks["raw"]()
    tasks["mart"]()

    assert len(commands) == 2
    assert commands[0][1] == "source"
    assert commands[1][1] == "run"
