from __future__ import annotations

import ast
from pathlib import Path


def _flows_tree() -> ast.Module:
    flows_path = Path(__file__).resolve().parents[2] / "sluice" / "dag" / "flows.py"
    return ast.parse(flows_path.read_text())


def _top_level_flow_names(tree: ast.Module) -> set[str]:
    found = set()
    for node in tree.body:
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            if (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "flow"
            ):
                found.add(node.name)
    return found


def test_flow_entrypoints_are_top_level_flow_functions():
    assert _top_level_flow_names(_flows_tree()) == {
        "run_pipeline_flow",
        "rotate_task_flow",
    }


def test_flow_names_are_ascii():
    for node in ast.walk(_flows_tree()):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            if not (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "flow"
            ):
                continue
            for keyword in decorator.keywords:
                if keyword.arg == "name":
                    assert isinstance(keyword.value, ast.Constant)
                    assert isinstance(keyword.value.value, str)
                    assert keyword.value.value.isascii()
