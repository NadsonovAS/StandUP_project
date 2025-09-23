"""Utilities for orchestrating the StandUP dbt transformation pipeline."""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Collection, Dict, List, Mapping, Sequence

logger = logging.getLogger(__name__)

RunCommand = Callable[[List[str]], subprocess.CompletedProcess[str]]


class DbtExecutionError(RuntimeError):
    """Raised when a dbt CLI invocation fails."""

    def __init__(
        self,
        step: "DbtStep",
        command: Sequence[str],
        result: subprocess.CompletedProcess[str],
    ) -> None:
        message = (
            "dbt step '%s' for layer '%s' failed with exit code %s.\nCommand: %s\nStdout: %s\nStderr: %s"
            % (
                step.name,
                step.layer,
                result.returncode,
                " ".join(command),
                result.stdout,
                result.stderr,
            )
        )
        super().__init__(message)
        self.step = step
        self.command = list(command)
        self.result = result


@dataclass(frozen=True)
class DbtStep:
    """Definition of a single dbt pipeline step."""

    name: str
    layer: str
    command: Sequence[str]
    description: str = ""
    vars: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.command:
            raise ValueError("DbtStep.command must not be empty")
        if self.layer == "":
            raise ValueError("DbtStep.layer must not be empty")
        if self.name == "":
            raise ValueError("DbtStep.name must not be empty")


@dataclass(frozen=True)
class DbtCommandResult:
    """Lightweight container for successful dbt CLI executions."""

    step: DbtStep
    command: List[str]
    stdout: str
    stderr: str
    return_code: int


class PipelineStateStore:
    """Persist the last successful execution timestamp for each pipeline layer."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._state: Dict[str, Any] = {"layers": {}}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text())
        except json.JSONDecodeError:
            logger.warning("State file %s is corrupted, starting from empty state", self.path)
            return
        if isinstance(data, Mapping) and isinstance(data.get("layers"), Mapping):
            self._state = {"layers": dict(data["layers"])}

    def get_last_run(self, layer: str) -> str | None:
        value = self._state.get("layers", {}).get(layer)
        if isinstance(value, str):
            return value
        return None

    def record_success(self, layer: str, timestamp: str) -> None:
        self._state.setdefault("layers", {})[layer] = timestamp
        self._write()

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._state, indent=2, sort_keys=True))


def _default_steps() -> List[DbtStep]:
    """Return the canonical StandUP dbt pipeline steps."""

    return [
        DbtStep(
            name="check_raw_sources",
            layer="raw",
            command=("source", "freshness", "--select", "standup_raw"),
            description="Validate availability of raw layer tables.",
        ),
        DbtStep(
            name="build_staging",
            layer="staging",
            command=("run", "--select", "path:models/staging"),
            description="Materialise staging views from raw sources.",
        ),
        DbtStep(
            name="build_core",
            layer="core",
            command=("run", "--select", "path:models/core"),
            description="Derive core tables consumed by marts.",
        ),
        DbtStep(
            name="build_mart",
            layer="mart",
            command=("run", "--select", "path:models/mart"),
            description="Publish final analytics marts.",
        ),
    ]


class DbtPipeline:
    """Orchestrate dbt CLI commands for the StandUP analytics stack."""

    def __init__(
        self,
        project_dir: Path | str,
        *,
        profiles_dir: Path | str | None = None,
        target: str | None = None,
        threads: int | None = None,
        steps: Sequence[DbtStep] | None = None,
        run_command: RunCommand | None = None,
        state_path: Path | None = None,
    ) -> None:
        self.project_dir = Path(project_dir).resolve()
        self.profiles_dir = Path(profiles_dir).resolve() if profiles_dir else None
        self.target = target
        self.threads = threads
        self.steps: List[DbtStep] = list(steps) if steps is not None else _default_steps()
        self._step_lookup = {step.layer: step for step in self.steps}
        if len(self._step_lookup) != len(self.steps):
            raise ValueError("Duplicate layer identifiers detected in steps configuration")

        self.run_command: RunCommand = run_command or self._default_runner
        default_state_path = self.project_dir.parent / "logs" / "dbt_pipeline_state.json"
        self.state_store = PipelineStateStore(state_path or default_state_path)

    @staticmethod
    def _default_runner(command: List[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(command, check=False, capture_output=True, text=True)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _build_command(
        self,
        step: DbtStep,
        *,
        full_refresh: bool,
        current_run: str,
        last_run: str | None,
    ) -> List[str]:
        command = ["dbt", *step.command]
        if full_refresh:
            command.append("--full-refresh")

        command.extend(["--project-dir", str(self.project_dir)])
        if self.profiles_dir is not None:
            command.extend(["--profiles-dir", str(self.profiles_dir)])
        if self.target is not None:
            command.extend(["--target", self.target])
        if self.threads is not None:
            command.extend(["--threads", str(self.threads)])

        combined_vars: Dict[str, Any] = {"standup_current_run": current_run}
        if last_run:
            combined_vars["standup_last_run"] = last_run
        if step.vars:
            combined_vars.update(step.vars)
        if combined_vars:
            command.extend(["--vars", json.dumps(combined_vars, sort_keys=True)])

        logger.debug("dbt command for step %s: %s", step.name, command)
        return command

    def _execute_step(
        self,
        step: DbtStep,
        *,
        full_refresh: bool,
        run_timestamp: str,
    ) -> DbtCommandResult:
        last_run = None if full_refresh else self.state_store.get_last_run(step.layer)
        command = self._build_command(
            step,
            full_refresh=full_refresh,
            current_run=run_timestamp,
            last_run=last_run,
        )

        logger.info("Running dbt step '%s' for layer '%s'", step.name, step.layer)
        result = self.run_command(command)

        if result.returncode != 0:
            logger.error(
                "dbt step '%s' for layer '%s' failed with code %s", step.name, step.layer, result.returncode
            )
            raise DbtExecutionError(step, command, result)

        self.state_store.record_success(step.layer, run_timestamp)
        logger.info("Completed dbt step '%s' for layer '%s'", step.name, step.layer)

        return DbtCommandResult(
            step=step,
            command=command,
            stdout=result.stdout,
            stderr=result.stderr,
            return_code=result.returncode,
        )

    def run(
        self,
        layers: Sequence[str] | None = None,
        *,
        full_refresh_layers: Collection[str] | None = None,
    ) -> List[DbtCommandResult]:
        """Execute the configured dbt steps sequentially."""

        selected_layers = set(layers) if layers is not None else {step.layer for step in self.steps}
        unknown_layers = selected_layers.difference(self._step_lookup)
        if unknown_layers:
            raise ValueError(f"Unknown layers requested: {sorted(unknown_layers)}")

        full_refresh = set(full_refresh_layers or ())
        run_timestamp = self._now_iso()

        results: List[DbtCommandResult] = []
        for step in self.steps:
            if step.layer not in selected_layers:
                continue
            result = self._execute_step(
                step,
                full_refresh=step.layer in full_refresh,
                run_timestamp=run_timestamp,
            )
            results.append(result)
        return results

    def run_layer(
        self,
        layer: str,
        *,
        full_refresh: bool = False,
        run_timestamp: str | None = None,
    ) -> DbtCommandResult:
        """Execute a single layer outside of the full pipeline run."""

        if layer not in self._step_lookup:
            raise ValueError(f"Layer '{layer}' is not configured for this pipeline")
        timestamp = run_timestamp or self._now_iso()
        return self._execute_step(
            self._step_lookup[layer],
            full_refresh=full_refresh,
            run_timestamp=timestamp,
        )

    def make_task_callables(
        self,
        *,
        full_refresh_layers: Collection[str] | None = None,
    ) -> Dict[str, Callable[[], DbtCommandResult]]:
        """Return callables for orchestrator integration (Airflow, Prefect, etc.)."""

        full_refresh = set(full_refresh_layers or ())
        tasks: Dict[str, Callable[[], DbtCommandResult]] = {}
        for step in self.steps:
            tasks[step.layer] = lambda s=step: self.run_layer(
                s.layer,
                full_refresh=s.layer in full_refresh,
            )
        return tasks


__all__ = [
    "DbtPipeline",
    "DbtStep",
    "DbtCommandResult",
    "DbtExecutionError",
]
