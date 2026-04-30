import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_hatch_build_targets_ship_sluice_package():
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    targets = data["tool"]["hatch"]["build"]["targets"]

    assert targets["wheel"]["packages"] == ["sluice"]
    assert "/sluice" in targets["sdist"]["include"]


def test_project_links_use_sluice_pipeline_repo():
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())

    assert data["project"]["urls"]["Homepage"] == "https://github.com/nerdneilsfield/sluice-pipeline"
    assert data["project"]["urls"]["Repository"] == "https://github.com/nerdneilsfield/sluice-pipeline"

    for rel_path in ("README.md", "README_ZH.md"):
        text = (ROOT / rel_path).read_text()
        assert "https://github.com/nerdneilsfield/sluice-pipeline" in text
        assert "https://github.com/nerdneilsfield/sluice/" not in text
        assert "https://github.com/nerdneilsfield/sluice)" not in text
        assert "github.com/nerdneilsfield/sluice.svg" not in text


def test_dockerfile_separates_dependency_and_project_installs():
    dockerfile = (ROOT / "scripts/docker/Dockerfile").read_text()

    assert "uv sync --no-dev --all-extras --frozen --no-install-project" in dockerfile
    assert 'ENTRYPOINT ["uv", "run", "--no-sync", "sluice"]' in dockerfile


def test_docker_examples_use_positional_pipeline_id():
    compose = (ROOT / "scripts/docker/docker-compose.yml").read_text()
    readme = (ROOT / "README.md").read_text()

    assert "--pipeline" not in compose
    assert "docker compose run --rm sluice run --pipeline" not in readme
    assert "sluice:local run --pipeline" not in readme


def test_workflows_use_locked_dependencies_and_release_guards():
    ci = (ROOT / ".github/workflows/ci.yml").read_text()
    publish = (ROOT / ".github/workflows/publish.yml").read_text()
    docker = (ROOT / ".github/workflows/docker.yml").read_text()

    assert "uv lock --check" in ci
    assert "uv sync --all-extras --frozen" in ci
    assert "uv lock --check" in publish
    assert "uv sync --all-extras --frozen" in publish
    assert "Verify tag matches package version" in publish
    assert "Verify tag matches package version" in docker
    assert "needs: verify" in docker
    assert "type=raw,value=latest" in docker
