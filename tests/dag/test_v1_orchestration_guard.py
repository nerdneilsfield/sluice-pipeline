from __future__ import annotations

from pathlib import Path

import pytest

FORBIDDEN_PREFIXES = (
    "from sluice.runner",
    "import sluice.runner",
    "from sluice.state",
    "import sluice.state",
    "from sluice.builders",
    "import sluice.builders",
    "from sluice.context",
    "import sluice.context",
    "from sluice.registry",
    "import sluice.registry",
)


def _stage_files():
    base = Path("sluice/dag/stages")
    return sorted(path for path in base.glob("*.py") if path.name != "__init__.py")


@pytest.mark.parametrize("path", _stage_files())
def test_stage_does_not_import_v1_orchestration(path):
    src = path.read_text()
    for prefix in FORBIDDEN_PREFIXES:
        assert prefix not in src, (
            f"{path}: imports v1 orchestration module ({prefix!r}); "
            "wrap algorithm only"
        )
