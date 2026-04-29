import pytest

from sluice.core.errors import ConfigError
from sluice.loader import validate_pipeline_attachment_email_compat


def test_email_with_file_prefix_raises():
    with pytest.raises(ConfigError) as exc:
        validate_pipeline_attachment_email_compat(
            sinks=[{"type": "email"}],
            stages=[{"type": "mirror_attachments"}],
            attachment_url_prefix="file:///tmp/x",
        )
    assert "email sink" in str(exc.value)


def test_email_with_https_prefix_passes():
    validate_pipeline_attachment_email_compat(
        sinks=[{"type": "email"}],
        stages=[{"type": "mirror_attachments"}],
        attachment_url_prefix="https://cdn/x",
    )


def test_email_without_mirror_stage_passes():
    validate_pipeline_attachment_email_compat(
        sinks=[{"type": "email"}],
        stages=[],
        attachment_url_prefix="file:///tmp/x",
    )
