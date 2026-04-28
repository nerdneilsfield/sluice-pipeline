from sluice.url_canon import canonical_url


def test_lowercase_scheme_host():
    assert canonical_url("HTTP://Example.COM/Path") == "http://example.com/Path"


def test_strip_fragment():
    assert canonical_url("https://x.com/a#frag") == "https://x.com/a"


def test_strip_tracking_params():
    assert (
        canonical_url("https://x.com/a?utm_source=foo&id=42&fbclid=z")
        == "https://x.com/a?id=42"
    )


def test_strip_all_known_trackers():
    u = "https://x.com/a?utm_source=a&utm_medium=b&utm_campaign=c&gclid=d&fbclid=e&ref=f&ref_src=g&id=42"
    assert canonical_url(u) == "https://x.com/a?id=42"


def test_keep_path_case():
    assert canonical_url("https://X.com/Foo/Bar") == "https://x.com/Foo/Bar"


def test_preserves_url_encoding():
    assert canonical_url("https://x.com/a?q=hello%20world") == "https://x.com/a?q=hello%20world"


def test_preserves_special_chars_in_values():
    assert canonical_url("https://x.com/a?k=a%26b") == "https://x.com/a?k=a%26b"
