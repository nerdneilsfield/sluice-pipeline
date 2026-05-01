from sluice.llm.tokens import (
    count_message_tokens,
    count_tokens,
    truncate_messages_to_tokens,
    truncate_to_tokens,
)


def test_count_tokens_empty_is_zero():
    assert count_tokens("") == 0


def test_count_tokens_short_string_positive():
    assert count_tokens("hello world") > 0


def test_count_tokens_grows_with_length():
    short = count_tokens("hello")
    long = count_tokens("hello " * 100)
    assert long > short * 50


def test_count_message_tokens_includes_overhead():
    """Per-message overhead means N empty messages count > 0."""
    msgs = [{"role": "user", "content": ""} for _ in range(3)]
    assert count_message_tokens(msgs) >= 3 * 4


def test_count_message_tokens_handles_list_content():
    msgs = [
        {"role": "user", "content": [{"type": "text", "text": "hello"}]},
    ]
    assert count_message_tokens(msgs) > 4


def test_truncate_to_tokens_short_unchanged():
    s = "hello"
    assert truncate_to_tokens(s, 1000) == s


def test_truncate_to_tokens_long_shrinks():
    s = "word " * 1000
    out = truncate_to_tokens(s, 50)
    assert count_tokens(out) <= 50
    assert len(out) < len(s)


def test_truncate_to_tokens_zero_returns_empty():
    assert truncate_to_tokens("anything", 0) == ""


def test_truncate_messages_keeps_earlier_intact():
    msgs = [
        {"role": "system", "content": "stay"},
        {"role": "user", "content": "word " * 5000},
    ]
    out = truncate_messages_to_tokens(msgs, 100)
    assert out[0]["content"] == "stay"
    assert count_message_tokens(out) <= 100 + 8


def test_truncate_messages_trims_earlier_messages_when_needed():
    msgs = [
        {"role": "system", "content": "system " * 5000},
        {"role": "user", "content": "small"},
    ]
    out = truncate_messages_to_tokens(msgs, 100)
    assert count_message_tokens(out) <= 100
    assert len(out[0]["content"]) < len(msgs[0]["content"])
