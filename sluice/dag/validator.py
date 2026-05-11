from __future__ import annotations

from collections import Counter, defaultdict, deque

from sluice.dag._name import safe_name
from sluice.dag.config import TaskConfig
from sluice.dag.registry import REGISTRY


class ValidationError(ValueError):
    pass


def _stage_type_of(task: TaskConfig, stage_name: str) -> str:
    for stage in task.stages:
        if stage.name == stage_name:
            return stage.type
    raise ValidationError(f"unknown stage reference: {stage_name!r}")


def _pipeline_names(task: TaskConfig) -> set[str]:
    return {pipeline.name for pipeline in task.pipelines}


def _check_unique_names(task: TaskConfig) -> None:
    for label, names in {
        "source": [source.name for source in task.sources],
        "sink": [sink.name for sink in task.sinks],
        "stage": [stage.name for stage in task.stages],
        "pipeline": [pipeline.name for pipeline in task.pipelines],
    }.items():
        duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
        if duplicates:
            raise ValidationError(f"duplicate {label} name: {', '.join(duplicates)}")


def _check_safe_names(task: TaskConfig) -> None:
    names_by_label = {
        "task": [task.name],
        "source": [source.name for source in task.sources],
        "sink": [sink.name for sink in task.sinks],
        "stage": [stage.name for stage in task.stages],
        "pipeline": [pipeline.name for pipeline in task.pipelines],
    }
    for label, names in names_by_label.items():
        for name in names:
            try:
                safe_name(name)
            except ValueError as exc:
                raise ValidationError(f"unsafe {label} name: {name!r}") from exc


def _check_stage_references(task: TaskConfig) -> None:
    source_names = {source.name for source in task.sources}
    for stage in task.stages:
        if stage.type not in REGISTRY:
            raise ValidationError(f"unknown stage type: {stage.type!r} (stage {stage.name!r})")
        for source_name in stage.params.get("sources", []) or []:
            if source_name not in source_names:
                raise ValidationError(
                    f"stage {stage.name!r} references unknown source {source_name!r}"
                )


def _check_shape(task: TaskConfig) -> None:
    for pipeline in task.pipelines:
        has_sources = bool(pipeline.sources)
        has_upstream = bool(pipeline.upstream_all) or bool(pipeline.upstream_any)
        if has_sources == has_upstream:
            raise ValidationError(
                f"pipeline {pipeline.name!r}: must have exactly one of sources or upstream"
            )
        if not pipeline.stages:
            raise ValidationError(f"pipeline {pipeline.name!r} has no stages")

        contains_fetch = any(
            _stage_type_of(task, stage_name).startswith("fetch_") for stage_name in pipeline.stages
        )
        if has_sources and not contains_fetch:
            raise ValidationError(
                f"pipeline {pipeline.name!r} declares sources but contains no fetch_<type> stage"
            )
        if has_upstream and contains_fetch:
            raise ValidationError(
                f"pipeline {pipeline.name!r} has upstream but contains a fetch_<type> stage"
            )

        last_type = _stage_type_of(task, pipeline.stages[-1])
        if REGISTRY[last_type].volatile and not pipeline.sinks:
            raise ValidationError(
                f"pipeline {pipeline.name!r} last stage "
                f"{pipeline.stages[-1]!r} is volatile ({last_type}) but pipeline "
                "has no sinks"
            )


def _check_pipeline_references(task: TaskConfig) -> None:
    source_names = {source.name for source in task.sources}
    sink_names = {sink.name for sink in task.sinks}
    stage_names = {stage.name for stage in task.stages}
    pipeline_names = _pipeline_names(task)

    for pipeline in task.pipelines:
        for stage_name in pipeline.stages:
            if stage_name not in stage_names:
                raise ValidationError(
                    f"pipeline {pipeline.name!r} references unknown stage {stage_name!r}"
                )
        for source_name in pipeline.sources or []:
            if source_name not in source_names:
                raise ValidationError(
                    f"pipeline {pipeline.name!r} references unknown source {source_name!r}"
                )
        for sink_name in pipeline.sinks or []:
            if sink_name not in sink_names:
                raise ValidationError(
                    f"pipeline {pipeline.name!r} references unknown sink {sink_name!r}"
                )
        for upstream_name in (pipeline.upstream_all or []) + (pipeline.upstream_any or []):
            if upstream_name not in pipeline_names:
                raise ValidationError(
                    f"pipeline {pipeline.name!r} references unknown upstream {upstream_name!r}"
                )

    if isinstance(task.deployments.pipelines, list):
        for pipeline_name in task.deployments.pipelines:
            if pipeline_name not in pipeline_names:
                raise ValidationError(f"deployments references unknown pipeline {pipeline_name!r}")


def _check_acyclic(task: TaskConfig) -> None:
    children_by_parent: dict[str, set[str]] = defaultdict(set)
    indegree = {pipeline.name: 0 for pipeline in task.pipelines}

    for pipeline in task.pipelines:
        for upstream_name in (pipeline.upstream_all or []) + (pipeline.upstream_any or []):
            if pipeline.name not in children_by_parent[upstream_name]:
                children_by_parent[upstream_name].add(pipeline.name)
                indegree[pipeline.name] += 1

    queue = deque(name for name, degree in indegree.items() if degree == 0)
    visited = 0
    while queue:
        name = queue.popleft()
        visited += 1
        for child_name in children_by_parent[name]:
            indegree[child_name] -= 1
            if indegree[child_name] == 0:
                queue.append(child_name)

    if visited != len(task.pipelines):
        raise ValidationError("DAG cycle detected in pipelines")


def validate_task(task: TaskConfig) -> None:
    _check_safe_names(task)
    _check_unique_names(task)
    _check_stage_references(task)
    _check_shape(task)
    _check_pipeline_references(task)
    _check_acyclic(task)


def compute_reach(task: TaskConfig, entry_pipeline: str) -> set[str]:
    pipeline_names = _pipeline_names(task)
    if entry_pipeline not in pipeline_names:
        raise ValidationError(f"unknown entry pipeline: {entry_pipeline!r}")

    if isinstance(task.deployments.pipelines, list):
        enabled = set(task.deployments.pipelines)
    else:
        enabled = pipeline_names

    children_by_parent: dict[str, set[str]] = defaultdict(set)
    for pipeline in task.pipelines:
        for upstream_name in (pipeline.upstream_all or []) + (pipeline.upstream_any or []):
            children_by_parent[upstream_name].add(pipeline.name)

    reach: set[str] = set()
    stack = [entry_pipeline]
    while stack:
        pipeline_name = stack.pop()
        if pipeline_name in reach:
            continue
        if pipeline_name not in enabled:
            continue
        reach.add(pipeline_name)
        stack.extend(children_by_parent[pipeline_name])
    return reach
