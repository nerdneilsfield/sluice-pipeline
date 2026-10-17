"""File Sink: confinement to file_root at load and at send, atomic writes."""

import os
import shutil
from datetime import UTC, datetime

import pytest

from sluice.app import open_app
from sluice.config import load_definition
from sluice.errors import DefinitionError, ExecutionError
from sluice.reconcile import tick
from sluice.sinks import file as file_sink

KEY = "cd" * 32


def source(config, checkpoint):
    from sluice.contracts import SourceRecord, SourceResult

    return SourceResult((SourceRecord("one", {"content": "Hello 世界"}),), "done")


def write_definition(base, directory="out", file_root=None, extra=""):
    root = f"file_root: {file_root}\n" if file_root else ""
    path = base / "d.yml"
    path.write_text(f"""version: 1
{root}{extra}sources:
  feed: {{use: tests.unit.test_file_sink.source}}
pipelines:
  p:
    nodes:
      out:
        from: source.feed
        deliver: {{use: sluice.sinks.file, with: {{directory: '{directory}'}}}}
""")
    return path


def send(root, payload="hello", **config):
    config.setdefault("directory", "out")
    return file_sink.send(config, payload, KEY, file_root=root)


def test_writes_key_named_file_and_rewrite_is_identical(tmp_path):
    assert send(tmp_path) == f"{KEY}.txt"
    target = tmp_path / "out" / f"{KEY}.txt"
    first = target.read_bytes()
    send(tmp_path)
    assert target.read_bytes() == first == b"hello"
    assert [p.name for p in (tmp_path / "out").iterdir()] == [target.name]
    assert target.stat().st_mode & 0o777 == 0o600


def test_md_extension_structured_payload_and_nested_directory(tmp_path):
    send(tmp_path, {"b": 1, "a": "é"}, directory="a/b", extension="md")
    assert (tmp_path / "a" / "b" / f"{KEY}.md").read_text() == '{"a":"é","b":1}'


def test_empty_payload_and_bad_options_are_errors(tmp_path):
    with pytest.raises(ExecutionError):
        send(tmp_path, "  ")
    with pytest.raises(ExecutionError):
        send(tmp_path, extension="exe")
    with pytest.raises(ExecutionError):
        send(tmp_path, bogus=1)
    assert not (tmp_path / "out").exists()


def test_oversize_content_is_rejected(tmp_path):
    with pytest.raises(ExecutionError, match="10 MiB"):
        send(tmp_path, "x" * (10 * 1024 * 1024 + 1))


def test_failure_at_replace_leaves_no_temp_or_partial_file(tmp_path, monkeypatch):
    def boom(*args, **kwargs):
        raise OSError(28, "No space left /secret/path")

    monkeypatch.setattr(file_sink.os, "replace", boom)
    with pytest.raises(ExecutionError) as caught:
        send(tmp_path)
    assert str(caught.value) == "File Sink write failed"
    assert list((tmp_path / "out").iterdir()) == []


def test_sender_rejects_directory_outside_root_and_swapped_symlink(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    for directory in ("../outside", str(outside)):
        with pytest.raises(ExecutionError, match="inside file_root"):
            send(root, directory=directory)
    (root / "out").symlink_to(outside)  # swapped in after load-time validation
    with pytest.raises(ExecutionError, match="inside file_root"):
        send(root)
    assert list(outside.iterdir()) == []


def test_sender_without_file_root_is_an_error():
    with pytest.raises(ExecutionError, match="file_root"):
        send(None)


def test_load_rejects_escapes_and_accepts_nested(tmp_path):
    for directory in ("../x", "/etc", "a/../../x"):
        with pytest.raises(DefinitionError, match="inside file_root"):
            load_definition(write_definition(tmp_path, directory))
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    (tmp_path / "link").symlink_to(outside)
    with pytest.raises(DefinitionError, match="inside file_root"):
        load_definition(write_definition(tmp_path, "link/sub"))
    definition = load_definition(write_definition(tmp_path, "a/b"))
    delivery = definition.pipelines["p"].nodes["out"].deliver
    assert delivery.with_config["directory"] == "a/b"  # authored, not resolved


def test_load_checks_directory_against_a_configured_file_root(tmp_path):
    (tmp_path / "data").mkdir()
    load_definition(write_definition(tmp_path, "reports", file_root="data"))
    load_definition(write_definition(tmp_path, str(tmp_path / "data" / "x"), "data"))
    with pytest.raises(DefinitionError, match="inside file_root"):
        load_definition(write_definition(tmp_path, "../elsewhere", file_root="data"))
    with pytest.raises(DefinitionError, match="file_root"):
        load_definition(write_definition(tmp_path, "x", file_root="''"))


def test_tick_writes_the_file_under_file_root(tmp_path):
    (tmp_path / "data").mkdir()
    app = open_app(
        write_definition(tmp_path, "reports", file_root="data"), tmp_path / "s.db"
    )
    try:
        tick(app, datetime(2026, 1, 1, tzinfo=UTC))
        key, state, external = app.conn.execute(
            "SELECT delivery_key,state,external_id FROM deliveries"
        ).fetchone()
    finally:
        app.close()
    assert state == "succeeded" and external == f"{key}.txt"
    assert (tmp_path / "data" / "reports" / external).read_text() == "Hello 世界"


def test_moving_the_project_does_not_change_delivery_keys(tmp_path):
    keys = []
    for name in ("first", "second"):
        base = tmp_path / name
        base.mkdir()
        app = open_app(
            write_definition(base, "reports", file_root="data"), base / "s.db"
        )
        try:
            tick(app, datetime(2026, 1, 1, tzinfo=UTC))
            keys.append(
                app.conn.execute(
                    "SELECT delivery_key,binding_digest,config_json FROM deliveries"
                ).fetchone()[:]
            )
        finally:
            app.close()
    assert keys[0][0] == keys[1][0] and keys[0][1] == keys[1][1]
    assert str(tmp_path) not in keys[0][2]


def test_changing_file_root_keeps_the_delivery_key(tmp_path):
    app = open_app(write_definition(tmp_path, "reports"), tmp_path / "s.db")
    try:
        tick(app, datetime(2026, 1, 1, tzinfo=UTC))
        before = app.conn.execute("SELECT delivery_key FROM deliveries").fetchone()[0]
    finally:
        app.close()
    shutil.rmtree(tmp_path / "reports")
    (tmp_path / "data").mkdir()
    app = open_app(
        write_definition(tmp_path, "reports", file_root="data"), tmp_path / "s.db"
    )
    try:
        tick(app, datetime(2026, 1, 1, 0, 1, tzinfo=UTC))
        rows = app.conn.execute("SELECT delivery_key FROM deliveries").fetchall()
    finally:
        app.close()
    assert [r[0] for r in rows] == [before]
    assert not os.path.exists(tmp_path / "data" / "reports")
