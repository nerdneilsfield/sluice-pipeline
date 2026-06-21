import logging

from loguru import logger

from sluice.core.logging_setup import configure_cli_logging, get_logger


def test_cli_logging_defaults_to_info_and_file_debug(tmp_path, capsys):
    log_file = tmp_path / "sluice.jsonl"
    configure_cli_logging(verbose=False, log_file=log_file)
    log = get_logger("sluice.test")

    log.debug("debug event")
    log.info("info event")
    logger.complete()

    captured = capsys.readouterr()
    assert "info event" in captured.err
    assert "debug event" not in captured.err
    text = log_file.read_text()
    assert "debug event" in text
    assert "info event" in text


def test_verbose_enables_console_debug(capsys):
    configure_cli_logging(verbose=True)
    get_logger("sluice.test").debug("debug event")
    logger.complete()

    captured = capsys.readouterr()
    assert "debug event" in captured.err


def test_console_logs_include_bound_context(capsys):
    configure_cli_logging(verbose=True)
    get_logger("sluice.fetchers.chain").bind(
        url="https://example.com/article",
        fetcher="trafilatura",
        error_class="ReadTimeout",
        error="timed out",
    ).debug("fetcher.attempt_failed")
    logger.complete()

    captured = capsys.readouterr()
    assert "fetcher.attempt_failed" in captured.err
    assert "url=https://example.com/article" in captured.err
    assert "fetcher=trafilatura" in captured.err
    assert "error_class=ReadTimeout" in captured.err
    assert "error=timed out" in captured.err


def test_verbose_exception_traceback_redacts_secret_locals(capsys):
    configure_cli_logging(verbose=True)
    log = get_logger("sluice.test")

    def fail_with_secret_local():
        notion_token = "secret_notion_token_123"
        raise RuntimeError(f"failed with token {notion_token}")

    try:
        fail_with_secret_local()
    except RuntimeError:
        log.opt(exception=True).error("operation failed")
        logger.complete()

    captured = capsys.readouterr()
    assert "operation failed" in captured.err
    assert "secret_notion_token_123" not in captured.err


def test_console_and_jsonl_logs_redact_secret_extra_values(tmp_path, capsys):
    log_file = tmp_path / "sluice.jsonl"
    configure_cli_logging(verbose=True, log_file=log_file)
    get_logger("sluice.test").bind(
        notion_token="secret_notion_token_456",
        smtp_password="secret_smtp_password_456",
        api_key="secret_api_key_456",
        authorization="Bearer secret_authorization_456",
        nested={"password": "secret_nested_password_456"},
    ).error("secret context")
    logger.complete()

    captured = capsys.readouterr()
    log_text = log_file.read_text()

    assert "secret context" in captured.err
    assert "secret context" in log_text
    for secret in [
        "secret_notion_token_456",
        "secret_smtp_password_456",
        "secret_api_key_456",
        "secret_authorization_456",
        "secret_nested_password_456",
    ]:
        assert secret not in captured.err
        assert secret not in log_text


def test_logs_redact_oauth_query_params_and_client_secret(capsys):
    configure_cli_logging(verbose=True)
    get_logger("sluice.test").error(
        "callback failed url=https://example.test/cb?access_token=access-123"
        "&refresh_token=refresh-456 client_secret=client-789"
    )
    logger.complete()

    captured = capsys.readouterr()
    assert "access-123" not in captured.err
    assert "refresh-456" not in captured.err
    assert "client-789" not in captured.err
    assert "access_token=<redacted>" in captured.err
    assert "refresh_token=<redacted>" in captured.err
    assert "client_secret=<redacted>" in captured.err


def test_third_party_debug_is_suppressed():
    configure_cli_logging(verbose=True)
    assert logging.getLogger("aiosqlite").getEffectiveLevel() == logging.WARNING
    assert logging.getLogger("httpx").getEffectiveLevel() == logging.WARNING
    assert logging.getLogger("httpcore").getEffectiveLevel() == logging.WARNING
