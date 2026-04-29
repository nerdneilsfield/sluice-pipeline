from sluice.core.item import Attachment


def test_attachment_has_local_path_field_default_none():
    a = Attachment(url="https://example.com/x.jpg")
    assert a.local_path is None


def test_attachment_local_path_settable():
    a = Attachment(url="2026/04/ab/abc.jpg", local_path="2026/04/ab/abc.jpg")
    assert a.local_path == "2026/04/ab/abc.jpg"
