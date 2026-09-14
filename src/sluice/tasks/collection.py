"""Deterministic, state-free collection Tasks."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import cmp_to_key
from typing import Any

from ..canonical import canonical_json, digest, utc_datetime
from ..contracts import DROP, FORWARD, BatchInput, Item
from ..errors import ExecutionError

_MISSING = object()


@dataclass(frozen=True, slots=True)
class _Task:
    input_mode: str
    function: Callable[[Mapping[str, Any], object], object]

    def run(self, config: Mapping[str, Any], input: object) -> object:
        return self.function(config, input)

    def __call__(self, config: Mapping[str, Any], input: object) -> object:
        return self.run(config, input)


def _field(item: Item, path: str) -> object:
    if not isinstance(item, Item):
        raise ExecutionError("collection Tasks require Item values")
    if not isinstance(path, str) or not path:
        raise ExecutionError("field must be a non-empty string")
    if path.startswith("$item."):
        name = path[len("$item.") :]
        stable = {
            "id": item.id,
            "logical_id": item.logical_id,
            "observed_at": item.observed_at,
            "content_hash": item.content_hash,
        }
        if name not in stable:
            raise ExecutionError(f"unsupported immutable Item field: {path}")
        return stable[name]
    value: object = item.payload
    for segment in path.split("."):
        if not isinstance(value, Mapping) or segment not in value:
            return _MISSING
        value = value[segment]
    return value


def _field_present(item: Item, path: str) -> tuple[bool, object]:
    value = _field(item, path)
    return (value is not _MISSING, value)


def field_value(item: Item, path: str) -> Any:
    """Read a payload or explicitly-prefixed immutable Item field.

    Missing payload fields are exposed as ``None`` for this small public
    convenience helper; the rule evaluator uses the private presence-aware
    variant so JSON ``null`` remains distinct from absence.
    """

    value = _field(item, path)
    return None if value is _MISSING else value


def _batch_values(input: object) -> tuple[tuple[Item, ...], tuple[Item, ...]]:
    if isinstance(input, BatchInput):
        values = input.items
        history = input.history
    elif isinstance(input, (tuple, list)):
        values = tuple(input)
        history = ()
    else:
        raise ExecutionError(
            "batch collection Task input must be BatchInput or sequence"
        )
    if any(not isinstance(item, Item) for item in values):
        raise ExecutionError("collection Tasks require Item values")
    if any(not isinstance(item, Item) for item in history):
        raise ExecutionError("dedup history must contain Item values")
    return tuple(values), tuple(history)


def _required_mapping(config: Mapping[str, Any], label: str) -> Mapping[str, Any]:
    if not isinstance(config, Mapping):
        raise ExecutionError(f"{label} configuration must be a mapping")
    return config


def _identity(item: Item, keys: Sequence[str]) -> str:
    values: list[object] = []
    for key in keys:
        present, value = _field_present(item, key)
        # Presence is part of comparison identity: a missing field, explicit
        # JSON null, and a payload object that happens to contain a sentinel
        # key are three distinct configured values.
        values.append({"present": present, "value": value if present else None})
    return digest({"keys": values})


def _order(item: Item) -> tuple[Any, str]:
    return (utc_datetime(item.observed_at), item.id)


def _validate_keys(config: Mapping[str, Any]) -> tuple[str, ...]:
    raw = config.get("keys")
    if not isinstance(raw, (list, tuple)) or not raw:
        raise ExecutionError("dedup.keys must be a non-empty list")
    keys = tuple(raw)
    if any(not isinstance(key, str) or not key for key in keys):
        raise ExecutionError("dedup.keys must contain non-empty strings")
    return keys


def _normalize_history_keys(keys: Sequence[str]) -> tuple[str, ...]:
    if not isinstance(keys, (list, tuple)) or not keys:
        raise ExecutionError("dedup.keys must be a non-empty list")
    normalized = tuple(keys)
    if any(not isinstance(key, str) or not key for key in normalized):
        raise ExecutionError("dedup.keys must contain non-empty strings")
    return normalized


def _levenshtein(left: str, right: str) -> int:
    # Python strings iterate over Unicode code points, exactly the metric the
    # Stage contract specifies (rather than UTF-8 bytes or grapheme clusters).
    if left == right:
        return 0
    if len(left) < len(right):
        left, right = right, left
    previous = list(range(len(right) + 1))
    for index, left_char in enumerate(left, 1):
        current = [index]
        for right_index, right_char in enumerate(right, 1):
            current.append(
                min(
                    current[-1] + 1,
                    previous[right_index] + 1,
                    previous[right_index - 1] + (left_char != right_char),
                )
            )
        previous = current
    return previous[-1]


def _change_rate(candidate: Item, prior: Item, config: Mapping[str, Any]) -> float:
    rule = config.get("change")
    if rule is None:
        return 1.0
    if not isinstance(rule, Mapping):
        raise ExecutionError("dedup.change must be a mapping")
    if set(rule) - {"field", "min_rate"}:
        raise ExecutionError("dedup.change contains unknown keys")
    field = rule.get("field")
    min_rate = rule.get("min_rate")
    if not isinstance(field, str) or not field:
        raise ExecutionError("dedup.change.field must be a non-empty string")
    if isinstance(min_rate, bool) or not isinstance(min_rate, (int, float)):
        raise ExecutionError("dedup.change.min_rate must be a number")
    if not math.isfinite(float(min_rate)) or not 0 <= float(min_rate) <= 1:
        raise ExecutionError("dedup.change.min_rate must be between 0 and 1")
    candidate_present, candidate_value = _field_present(candidate, field)
    prior_present, prior_value = _field_present(prior, field)
    if not candidate_present or not prior_present:
        raise ExecutionError("dedup change field must exist on both Items")
    if not isinstance(candidate_value, str) or not isinstance(prior_value, str):
        raise ExecutionError("dedup change field values must be strings")
    denominator = max(len(candidate_value), len(prior_value))
    rate = (
        0.0
        if denominator == 0
        else _levenshtein(candidate_value, prior_value) / denominator
    )
    return rate


def _prior_decision(
    candidate: Item, prior: Item | None, config: Mapping[str, Any]
) -> bool:
    if prior is None:
        rule = config.get("change")
        if rule is not None:
            if not isinstance(rule, Mapping):
                raise ExecutionError("dedup.change must be a mapping")
            # Validate the configured field even when there is no prior.  The
            # rate is then the contract's defined 1.0, but a typo in the
            # candidate field remains a Stage error.
            field = rule.get("field")
            if not isinstance(field, str) or not field:
                raise ExecutionError("dedup.change.field must be a non-empty string")
            if set(rule) - {"field", "min_rate"}:
                raise ExecutionError("dedup.change contains unknown keys")
            min_rate = rule.get("min_rate")
            if isinstance(min_rate, bool) or not isinstance(min_rate, (int, float)):
                raise ExecutionError("dedup.change.min_rate must be a number")
            if not math.isfinite(float(min_rate)) or not 0 <= float(min_rate) <= 1:
                raise ExecutionError("dedup.change.min_rate must be between 0 and 1")
            present, _ = _field_present(candidate, field)
            if not present:
                raise ExecutionError("dedup change field must exist on candidate Item")
        return True
    # Metadata changes are independently material, so they intentionally
    # bypass the payload change-rate threshold (including when both payload
    # and metadata changed).  Only a payload-only change is rate-gated.
    if candidate.metadata != prior.metadata:
        return True
    if candidate.payload == prior.payload:
        return False
    if config.get("change") is None:
        return True
    return _change_rate(candidate, prior, config) >= float(config["change"]["min_rate"])


def dedup(config: Mapping[str, Any], input: object) -> tuple[object, ...]:
    """Forward only the newest materially changed Item per configured identity."""

    config = _required_mapping(config, "dedup")
    keys = _validate_keys(config)
    candidates, history = _batch_values(input)
    if not candidates:
        return ()

    current_groups: dict[str, list[tuple[int, Item]]] = {}
    for index, candidate in enumerate(candidates):
        current_groups.setdefault(_identity(candidate, keys), []).append(
            (index, candidate)
        )
    history_groups: dict[str, list[Item]] = {}
    for prior in history:
        history_groups.setdefault(_identity(prior, keys), []).append(prior)

    selected: dict[str, tuple[int, Item]] = {}
    for identity, rows in current_groups.items():
        selected[identity] = max(rows, key=lambda row: (_order(row[1]), row[0]))
    prior_selected = {
        identity: max(rows, key=_order) for identity, rows in history_groups.items()
    }

    output: list[object] = []
    for index, candidate in enumerate(candidates):
        identity = _identity(candidate, keys)
        winner_index, winner = selected[identity]
        prior = prior_selected.get(identity)
        if index != winner_index:
            output.append(DROP)
            continue
        output.append(
            FORWARD(candidate) if _prior_decision(candidate, prior, config) else DROP
        )
    return tuple(output)


def select_dedup_history(
    candidates: Sequence[Item],
    accepted_rows: Sequence[object],
    *,
    keys: Sequence[str] | None = None,
) -> tuple[Item, ...]:
    """Filter immutable Stage-result rows against the current input envelope.

    SQL performs the Stage/name and reusable-result join.  This helper keeps
    the remaining envelope exclusion and deterministic latest-per-identity
    choice pure and easy to exercise without a repository seam.
    """

    candidate_ids: set[str] = set()
    candidate_order: list[str] = []
    candidate_winners: dict[str, Item] = {}
    normalized_keys = None if keys is None else _normalize_history_keys(keys)
    for candidate in candidates:
        if not isinstance(candidate, Item):
            raise ExecutionError("dedup candidates must contain Item values")
        candidate_ids.add(candidate.id)
        identity = (
            _identity(candidate, normalized_keys)
            if normalized_keys is not None
            else digest(
                {
                    "producer_scope": candidate.producer_scope,
                    "logical_id": candidate.logical_id,
                }
            )
        )
        if identity not in candidate_winners:
            candidate_order.append(identity)
            candidate_winners[identity] = candidate
        elif _order(candidate) > _order(candidate_winners[identity]):
            candidate_winners[identity] = candidate
    rows: list[tuple[Item, tuple[str, ...]]] = []
    for row in accepted_rows:
        if isinstance(row, Mapping):
            item = row.get("item")
            input_ids = row.get("input_ids", ())
        elif isinstance(row, (tuple, list)) and len(row) == 2:
            item, input_ids = row
        else:
            raise ExecutionError("dedup history rows must contain Item and input IDs")
        if not isinstance(item, Item) or not isinstance(input_ids, (tuple, list)):
            raise ExecutionError("dedup history rows must contain Item and input IDs")
        immutable_ids = tuple(input_ids)
        if any(not isinstance(value, str) for value in immutable_ids):
            raise ExecutionError("dedup history input IDs must be strings")
        if candidate_ids.intersection(immutable_ids) or item.id in candidate_ids:
            continue
        rows.append((item, immutable_ids))

    if normalized_keys is None:
        # Preserve the original two-argument pure helper contract for callers
        # that do not have a configured dedup Stage.  The durable resolver
        # always supplies keys and takes the stricter candidate-aware path
        # below.
        first_order: list[str] = []
        legacy_selected: dict[str, Item] = {}
        for item, _ in rows:
            identity = digest(
                {"producer_scope": item.producer_scope, "logical_id": item.logical_id}
            )
            if identity not in legacy_selected:
                first_order.append(identity)
                legacy_selected[identity] = item
            elif _order(item) > _order(legacy_selected[identity]):
                legacy_selected[identity] = item
        return tuple(legacy_selected[identity] for identity in first_order)

    selected: dict[str, Item] = {}
    for item, _ in rows:
        identity = (
            _identity(item, normalized_keys)
            if normalized_keys is not None
            else digest(
                {"producer_scope": item.producer_scope, "logical_id": item.logical_id}
            )
        )
        candidate = candidate_winners.get(identity)
        if candidate is None or _order(item) >= _order(candidate):
            continue
        if identity not in selected or _order(item) > _order(selected[identity]):
            selected[identity] = item
    return tuple(
        selected[identity] for identity in candidate_order if identity in selected
    )


def _kind(value: object) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, (list, tuple)):
        return "array"
    return type(value).__name__


def _compatible(left: object, right: object) -> bool:
    left_kind, right_kind = _kind(left), _kind(right)
    return left_kind == right_kind or {left_kind, right_kind} <= {"number"}


def _require_comparable(left: object, right: object) -> None:
    if left is None or right is None:
        return
    if not _compatible(left, right):
        raise ExecutionError(
            f"incompatible non-null filter values: {_kind(left)} and {_kind(right)}"
        )


def _equal(left: object, right: object) -> bool:
    if left is None or right is None:
        return False
    _require_comparable(left, right)
    if _kind(left) in {"array", "object"}:
        # Item payload containers are deeply frozen into tuples/mappingproxy,
        # while validated Task config is thawed into lists/dicts.  Canonical
        # JSON equality compares those equivalent JSON shapes correctly.
        return canonical_json(left) == canonical_json(right)
    return left == right


def _rule_result(rule: object, item: Item) -> bool:
    if not isinstance(rule, Mapping):
        raise ExecutionError("filter rules must be mappings")
    if "all" in rule:
        if set(rule) != {"all"} or not isinstance(rule["all"], (list, tuple)):
            raise ExecutionError("filter.all must be a list of rules")
        return all(_rule_result(child, item) for child in rule["all"])
    if "any" in rule:
        if set(rule) != {"any"} or not isinstance(rule["any"], (list, tuple)):
            raise ExecutionError("filter.any must be a list of rules")
        return any(_rule_result(child, item) for child in rule["any"])
    if "not" in rule:
        if set(rule) != {"not"}:
            raise ExecutionError("filter.not must contain one rule")
        return not _rule_result(rule["not"], item)
    if set(rule) - {"field", "op", "value"} or "field" not in rule or "op" not in rule:
        raise ExecutionError("filter leaf must contain field and op")
    field = rule["field"]
    op = rule["op"]
    if not isinstance(field, str) or not isinstance(op, str):
        raise ExecutionError("filter field and op must be strings")
    present, actual = _field_present(item, field)
    if op == "exists":
        if "value" in rule:
            raise ExecutionError("filter.exists does not accept value")
        return present
    if not present or actual is None:
        return False
    if "value" not in rule:
        raise ExecutionError(f"filter.{op} requires value")
    expected = rule["value"]
    if op in {"eq", "ne", "gt", "gte", "lt", "lte"}:
        if expected is None:
            return False
        _require_comparable(actual, expected)
        if op == "eq":
            return _equal(actual, expected)
        if op == "ne":
            return not _equal(actual, expected)
        try:
            return {
                "gt": actual > expected,
                "gte": actual >= expected,
                "lt": actual < expected,
                "lte": actual <= expected,
            }[op]
        except TypeError as error:
            raise ExecutionError("filter values are not orderable") from error
    if op in {"in", "not_in"}:
        if not isinstance(expected, (list, tuple)):
            raise ExecutionError(f"filter.{op} value must be a list")
        matches = False
        for candidate in expected:
            if candidate is None:
                continue
            _require_comparable(actual, candidate)
            matches = matches or _equal(actual, candidate)
        return matches if op == "in" else not matches
    if op == "contains":
        if expected is None:
            return False
        if isinstance(actual, str):
            if not isinstance(expected, str):
                raise ExecutionError("filter.contains requires matching value types")
            return expected in actual
        if isinstance(actual, Mapping):
            if not isinstance(expected, str):
                raise ExecutionError("filter.contains object key must be a string")
            return expected in actual
        if isinstance(actual, (list, tuple)):
            return any(_equal(value, expected) for value in actual)
        raise ExecutionError("filter.contains requires string, array, or object")
    raise ExecutionError(f"unsupported filter operation: {op}")


def filter(config: Mapping[str, Any], input: object) -> tuple[object, ...]:
    config = _required_mapping(config, "filter")
    if set(config) != {"all"} and set(config) != {"any"} and set(config) != {"not"}:
        # A leaf is also accepted as a compact one-rule configuration.
        if not ({"field", "op"} <= set(config)):
            raise ExecutionError("filter configuration must contain one rule")
        rule: object = config
    else:
        rule = config
    items, _ = _batch_values(input)
    return tuple(FORWARD(item) if _rule_result(rule, item) else DROP for item in items)


def _sort_rules(config: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw = config.get("by")
    if not isinstance(raw, (list, tuple)) or not raw:
        raise ExecutionError("sort.by must be a non-empty list")
    rules: list[Mapping[str, Any]] = []
    for rule in raw:
        if not isinstance(rule, Mapping):
            raise ExecutionError("sort rules must be mappings")
        if set(rule) - {"field", "order", "nulls"}:
            raise ExecutionError("sort rule contains unknown keys")
        field_name = rule.get("field")
        order = rule.get("order", "asc")
        nulls = rule.get("nulls", "last")
        if not isinstance(field_name, str) or not field_name:
            raise ExecutionError("sort.field must be a non-empty string")
        if order not in {"asc", "desc"} or nulls not in {"first", "last"}:
            raise ExecutionError("sort order/nulls must be asc|desc and first|last")
        rules.append({"field": field_name, "order": order, "nulls": nulls})
    return tuple(rules)


def sort(config: Mapping[str, Any], input: object) -> tuple[object, ...]:
    config = _required_mapping(config, "sort")
    rules = _sort_rules(config)
    items, _ = _batch_values(input)

    def compare(left: Item, right: Item) -> int:
        for rule in rules:
            left_present, left_value = _field_present(left, rule["field"])
            right_present, right_value = _field_present(right, rule["field"])
            left_null = not left_present or left_value is None
            right_null = not right_present or right_value is None
            if left_null or right_null:
                if left_null and right_null:
                    continue
                result = -1 if left_null == (rule["nulls"] == "first") else 1
                return result
            _require_comparable(left_value, right_value)
            if left_value == right_value:
                continue
            try:
                result = -1 if left_value < right_value else 1
            except TypeError as error:
                raise ExecutionError("sort values are not orderable") from error
            return -result if rule["order"] == "desc" else result
        return -1 if left.id < right.id else (1 if left.id > right.id else 0)

    ordered = sorted(items, key=cmp_to_key(compare))
    return tuple(FORWARD(item) for item in ordered)


def limit(config: Mapping[str, Any], input: object) -> tuple[object, ...]:
    config = _required_mapping(config, "limit")
    count = config.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ExecutionError("limit.count must be a non-negative integer")
    items, _ = _batch_values(input)
    return tuple(FORWARD(item) for item in items[:count])


dedup = _Task("batch", dedup)
filter = _Task("batch", filter)
sort = _Task("batch", sort)
limit = _Task("batch", limit)
