"""Optional Prefect wrapper around the ordinary Sluice tick function."""

from __future__ import annotations

from datetime import UTC, datetime

from .app import open_app
from .canonical import utc_datetime
from .reconcile import tick

try:  # Prefect is intentionally optional for local execution.
    from prefect import flow
except ImportError:  # pragma: no cover - depends on the optional extra.
    flow = None


if flow is None:  # pragma: no cover - exercised only without the optional extra.

    def prefect_tick(
        definition_path: str,
        db_path: str | None = None,
        at: datetime | None = None,
    ) -> None:
        """Report that the optional Prefect dependency is not installed."""

        raise RuntimeError(
            "Prefect execution requires the optional dependency; "
            "install sluice-pipeline[prefect]"
        )

else:

    @flow(name="sluice-tick")
    def prefect_tick(
        definition_path: str,
        db_path: str | None = None,
        at: datetime | None = None,
    ) -> None:
        """Run one Sluice tick through Prefect without a second state machine."""

        app = open_app(definition_path, db_path)
        try:
            tick(
                app,
                utc_datetime(at) if at is not None else datetime.now(UTC),
            )
        finally:
            app.close()


__all__ = ["prefect_tick"]
