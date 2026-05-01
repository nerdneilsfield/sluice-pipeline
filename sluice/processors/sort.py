from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from numbers import Real

from sluice.context import PipelineContext
from sluice.core.errors import ConfigError
from sluice.core.item import Item

_MISSING = object()


@dataclass
class SortProcessor:
    name: str
    sort_by: str
    sort_type: str = "auto"
    sort_order: str = "desc"
    sort_missing: str = "last"

    def _coerce_number(self, value) -> float:
        if isinstance(value, bool):
            raise ConfigError(f"sort stage {self.name}: unsupported bool value for {self.sort_by}")
        if isinstance(value, Real):
            number = float(value)
        else:
            try:
                number = float(value)
            except (TypeError, ValueError) as exc:
                raise ConfigError(
                    f"sort stage {self.name}: value for {self.sort_by} is not numeric"
                ) from exc
        if not math.isfinite(number):
            raise ConfigError(
                f"sort stage {self.name}: non-finite numeric value for {self.sort_by}"
            )
        return number

    def _coerce_datetime(self, value) -> float:
        if isinstance(value, datetime):
            return value.timestamp()
        if isinstance(value, date):
            midnight = datetime(value.year, value.month, value.day)
            return midnight.timestamp()
        raise ConfigError(
            f"sort stage {self.name}: value for {self.sort_by} is not a date/datetime"
        )

    def _coerce_auto(self, value):
        if isinstance(value, str):
            try:
                return "number", self._coerce_number(value)
            except ConfigError:
                return "string", value
        if isinstance(value, datetime | date):
            return "datetime", self._coerce_datetime(value)
        if isinstance(value, Real):
            return "number", self._coerce_number(value)
        raise ConfigError(
            f"sort stage {self.name}: unsupported value type "
            f"{type(value).__name__} for {self.sort_by}"
        )

    def _coerce_value(self, value):
        if self.sort_type == "number":
            return "number", self._coerce_number(value)
        if self.sort_type == "string":
            return "string", str(value)
        if self.sort_type == "datetime":
            return "datetime", self._coerce_datetime(value)
        return self._coerce_auto(value)

    def _split_items(self, items: list[Item]):
        present: list[tuple[Item, object]] = []
        missing: list[Item] = []
        for item in items:
            val = item.get(self.sort_by, _MISSING)
            if val is _MISSING or val is None:
                missing.append(item)
            else:
                present.append((item, val))
        return present, missing

    def _sort_present(self, present: list[tuple[Item, object]]) -> list[Item]:
        coerced = []
        seen_types: set[str] = set()
        for item, value in present:
            value_type, sort_value = self._coerce_value(value)
            coerced.append((item, value_type, sort_value))
            seen_types.add(value_type)

        if len(seen_types) > 1:
            raise ConfigError(
                f"sort stage {self.name}: mixed sort value types for {self.sort_by}: "
                f"{', '.join(sorted(seen_types))}"
            )

        reverse = self.sort_order == "desc"
        return [
            item
            for item, _, _ in sorted(
                coerced,
                key=lambda row: row[2],
                reverse=reverse,
            )
        ]

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        items = list(ctx.items)
        present, missing = self._split_items(items)
        sorted_present = self._sort_present(present)

        if self.sort_missing == "drop":
            ctx.items = sorted_present
        elif self.sort_missing == "first":
            ctx.items = missing + sorted_present
        else:
            ctx.items = sorted_present + missing
        return ctx
