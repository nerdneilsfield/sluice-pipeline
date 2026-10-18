import pytest

from sluice import db


def test_unopenable_lock_file_closes_the_connection(tmp_path, monkeypatch):
    closed = []
    close = db.LockedConnection.close

    def recording_close(self):
        closed.append(self)
        close(self)

    monkeypatch.setattr(db.LockedConnection, "close", recording_close)
    (tmp_path / "state.db.lock").mkdir()

    with pytest.raises(IsADirectoryError):
        db.open_database(tmp_path / "state.db")

    assert len(closed) == 1
