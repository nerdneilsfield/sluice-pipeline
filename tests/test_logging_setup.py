import logging

from loguru import logger

from sluice.logging_setup import configure_cli_logging, get_logger


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


def test_third_party_debug_is_suppressed():
    configure_cli_logging(verbose=True)
    assert logging.getLogger("httpx").getEffectiveLevel() == logging.WARNING
    assert logging.getLogger("httpcore").getEffectiveLevel() == logging.WARNING
