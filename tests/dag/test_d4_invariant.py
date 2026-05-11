from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "path",
    [
        "sluice/dag/phases/tx1.py",
        "sluice/dag/phases/tx2.py",
        "sluice/dag/emission.py",
        "sluice/dag/cleanup.py",
        "sluice/dag/rotation/lock.py",
        "sluice/dag/rotation/admission.py",
        "sluice/dag/rotation/gate.py",
        "sluice/dag/rotation/drain.py",
        "sluice/dag/rotation/reach.py",
        "sluice/dag/rotation/partition.py",
        "sluice/dag/rotation/new_active.py",
        "sluice/dag/rotation/copy.py",
        "sluice/dag/rotation/swap.py",
        "sluice/dag/rotation/orchestrator.py",
    ],
)
def test_d4_no_seen_writes_outside_seen_post(path):
    src = Path(path).read_text()
    assert "ItemSeenStore" not in src, (
        "Seen writes are confined to sluice/dag/phases/seen_post.py only."
    )
    assert "INTO item_seen" not in src, (
        "Seen writes are confined to sluice/dag/phases/seen_post.py only."
    )


def test_d4_seen_post_only_writes_seen():
    src = Path("sluice/dag/phases/seen_post.py").read_text()
    assert "ItemSeenStore" in src
    assert "insert_many" in src
    for forbidden in (
        "INTO items",
        "INTO pipelines",
        "INTO stages",
        "INTO sink_emissions",
        "INTO sink_delivery_log",
    ):
        assert forbidden not in src


def test_d4_seen_reconcile_only_writes_seen():
    src = Path("sluice/dag/rotation/seen_reconcile.py").read_text()
    assert "ItemSeenStore" in src
    assert "insert_many" in src
    for forbidden in (
        "INTO items",
        "INTO pipelines",
        "INTO stages",
        "INTO sink_emissions",
        "INTO sink_delivery_log",
    ):
        assert forbidden not in src
