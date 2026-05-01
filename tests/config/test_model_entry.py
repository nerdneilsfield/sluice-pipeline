from sluice.config import ModelEntry


def test_model_entry_default_token_caps():
    m = ModelEntry(model_name="x")
    assert m.max_input_tokens == 128_000
    assert m.max_output_tokens == 4_096


def test_model_entry_overridden_token_caps():
    m = ModelEntry(model_name="big", max_input_tokens=1_000_000, max_output_tokens=8_192)
    assert m.max_input_tokens == 1_000_000
    assert m.max_output_tokens == 8_192
