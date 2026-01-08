import pathlib
import sys
import warnings
from dataclasses import dataclass
from typing import Any, List

import pytest

# Ensure the project root (with the src package) is on the Python path for tests
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

warnings.filterwarnings("ignore", category=DeprecationWarning, module="pysbd")


@dataclass
class FunctionalRun:
    feature: str
    details: str


class ExecutionLog:
    def __init__(self) -> None:
        self.entries: List[FunctionalRun] = []

    def record(self, feature: str, details: str) -> None:
        self.entries.append(FunctionalRun(feature=feature, details=details))


def pytest_configure(config: pytest.Config) -> None:
    config.execution_log = ExecutionLog()


@pytest.fixture(scope="session")
def execution_log(pytestconfig: pytest.Config) -> ExecutionLog:
    return pytestconfig.execution_log


def pytest_terminal_summary(
    terminalreporter: Any, exitstatus: int
) -> None:  # type: ignore[override]
    log: ExecutionLog | None = getattr(terminalreporter.config, "execution_log", None)
    if not log or not log.entries:
        return

    terminalreporter.write_sep("=", "Functional scenario highlights")
    for entry in log.entries:
        terminalreporter.write_line(f"- {entry.feature}: {entry.details}")
