from typer.testing import CliRunner

from sluice.cli import app

runner = CliRunner()


def _write_min_config(cfg_dir, *, gc_enabled: bool, db_path, with_pipeline: bool = True):
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / "pipelines").mkdir(exist_ok=True)
    body = (
        f'[state]\ndb_path = "{db_path}"\nattachment_dir = "/tmp/att"\nattachment_url_prefix = ""\n'
    )
    if gc_enabled:
        body += '[gc.schedule]\nenabled = true\ncron = "0 3 * * *"\n'
    (cfg_dir / "sluice.toml").write_text(body)
    (cfg_dir / "providers.toml").write_text(
        "[[providers]]\nname='dummy'\ntype='openai_compatible'\nbase=[]\nmodels=[]\n"
    )
    if with_pipeline:
        (cfg_dir / "pipelines" / "p1.toml").write_text(
            'id = "p1"\ncron = "0 8 * * *"\n'
            '[[sources]]\ntype="rss"\nurl="https://example.com/feed.xml"\n'
            '[[stages]]\nname="d"\ntype="dedupe"\n'
            '[[sinks]]\nid="f"\ntype="file_md"\ninput="context.md"\npath="/tmp/out.md"\n'
        )


def test_deploy_with_gc_disabled_does_not_register_gc(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "configs"
    _write_min_config(cfg_dir, gc_enabled=False, db_path=tmp_path / "s.db")
    captured: list = []

    def _fake_serve(*deployments, **kw):
        captured.extend(deployments)

    monkeypatch.setattr("sluice.cli.prefect_serve", _fake_serve)
    res = runner.invoke(app, ["deploy", "--config-dir", str(cfg_dir)])
    assert res.exit_code == 0, res.stdout
    names = [d.name for d in captured]
    assert "sluice-p1" in names
    assert "sluice-gc" not in names


def test_deploy_with_gc_enabled_registers_gc_alongside_pipelines(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "configs"
    _write_min_config(cfg_dir, gc_enabled=True, db_path=tmp_path / "s.db")
    captured: list = []

    def _fake_serve(*deployments, **kw):
        captured.extend(deployments)

    monkeypatch.setattr("sluice.cli.prefect_serve", _fake_serve)
    res = runner.invoke(app, ["deploy", "--config-dir", str(cfg_dir)])
    assert res.exit_code == 0, res.stdout
    names = [d.name for d in captured]
    assert "sluice-p1" in names
    assert "sluice-gc" in names
