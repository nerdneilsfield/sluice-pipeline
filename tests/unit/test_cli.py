import sys

import pytest

from sluice import cli

AT = "2026-08-14T00:00:00Z"


def test_definition_error_is_reported_without_a_traceback(tmp_path, capsys):
    definition = tmp_path / "bad.yml"
    definition.write_text("version: 2\n", encoding="utf-8")

    assert cli.main(["tick", str(definition), "--at", AT]) == 2

    err = capsys.readouterr().err
    assert err.startswith("sluice: error: ")
    assert "Traceback" not in err


def test_execution_error_is_reported_without_a_traceback(tmp_path, capsys):
    definition = tmp_path / "ok.yml"
    definition.write_text("version: 1\ndatabase: state.db\n", encoding="utf-8")

    assert cli.main(["reconcile", str(definition), "p", "n", "--at", AT]) == 2

    err = capsys.readouterr().err
    assert err.startswith("sluice: error: unknown Pipeline node: p.n")
    assert "Traceback" not in err


@pytest.mark.parametrize("port", ["0", "65536", "-1", "abc"])
def test_ui_rejects_an_invalid_port(port, capsys):
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["ui", "definition.yml", "--port", port])

    assert exit_info.value.code == 2
    assert "port must be an integer from 1 to 65535" in capsys.readouterr().err


def test_ui_without_the_extra_fails_before_the_app_is_opened(monkeypatch, capsys):
    monkeypatch.setitem(sys.modules, "flask", None)

    def open_app(*args, **kwargs):
        raise AssertionError("open_app must not run when the UI cannot start")

    monkeypatch.setattr(cli, "open_app", open_app)

    assert cli.main(["ui", "definition.yml"]) == 2
    assert "ui extra" in capsys.readouterr().err
