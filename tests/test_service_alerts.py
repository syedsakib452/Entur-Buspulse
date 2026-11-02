"""A change in the alerts must not be forgotten if its save is skipped. Run: pytest"""
import collectors.service_alerts as sa


class FakeClient:
    def __init__(self, payload):
        self.payload = payload

    def get_siri(self, url):
        return self.payload


ALERTS = b"<Siri><PtSituationElement><SituationNumber>1</SituationNumber></PtSituationElement></Siri>"


def test_changed_alerts_are_retried_if_save_was_skipped(monkeypatch):
    results = iter(["skipped", "swift"])
    saved = []
    monkeypatch.setattr(sa.storage, "save", lambda *a, **k: (saved.append(a), next(results))[1])

    fp = sa.collect(FakeClient(ALERTS), None)
    assert fp is None  # skipped: still counts as "not saved yet"
    fp = sa.collect(FakeClient(ALERTS), fp)
    assert fp == sa.fingerprint(ALERTS) and len(saved) == 2  # saved on the next round


def test_unchanged_alerts_are_not_saved_again(monkeypatch):
    saved = []
    monkeypatch.setattr(sa.storage, "save", lambda *a, **k: (saved.append(a), "swift")[1])
    fp = sa.collect(FakeClient(ALERTS), None)
    sa.collect(FakeClient(ALERTS), fp)
    assert len(saved) == 1
