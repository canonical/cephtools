from __future__ import annotations

import json

from cephtools import juju_utils


def test_application_machines_returns_sorted_unique(monkeypatch) -> None:
    calls: dict[str, object] = {}

    payload = {
        "applications": {
            "microceph": {
                "units": {
                    "microceph/0": {"machine": "3"},
                    "microceph/1": {"machine": "1"},
                    "microceph/2": {"machine": "not-a-number"},
                    "microceph/3": {},
                    "microceph/4": {"machine": 3},
                }
            }
        }
    }

    class FakeJuju:
        def __init__(self, *, model: str):
            calls.setdefault("models", []).append(model)

        def cli(self, *args: str) -> str:
            calls.setdefault("cli_calls", []).append(args)
            return json.dumps(payload)

    monkeypatch.setattr(juju_utils.jubilant, "Juju", FakeJuju)

    machines = juju_utils.application_machines("ceph-model", "microceph")

    assert machines == (1, 3)
    assert calls["models"] == ["ceph-model"]
    assert calls["cli_calls"] == [("status", "--format", "json")]


def test_application_machines_missing_app(monkeypatch) -> None:
    class FakeJuju:
        def __init__(self, *, model: str):
            self.model = model

        def cli(self, *args: str) -> str:
            return json.dumps({"applications": {}})

    monkeypatch.setattr(juju_utils.jubilant, "Juju", FakeJuju)

    machines = juju_utils.application_machines("ceph-model", "microceph")

    assert machines == ()
