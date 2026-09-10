from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "swarm_backend", Path(__file__).parents[1] / "scripts/swarm_backend.py"
)
assert SPEC and SPEC.loader
backend = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backend)
IMAGE = "192.168.1.197:5443/tradix/worker-canary@sha256:" + "a" * 64


def source():
    return {
        "image": IMAGE,
        "read_only": True,
        "security_opt": ["no-new-privileges:true"],
        "networks": {"developer-plane": None},
        "volumes": [
            {"type": "bind", "source": "/host_mnt/d/canary/home", "target": "/home/hermes"},
            {
                "type": "bind",
                "source": "/host_mnt/d/dataset",
                "target": "/datasets/tradix",
                "read_only": True,
            },
        ],
        "environment": {
            "HERMES_AGENT_ID": "developer",
            "OPENAI_BASE_URL": "http://codex-broker:8650/v1",
        },
        "healthcheck": {"test": ["CMD", "true"], "interval": "3s"},
        "stop_grace_period": "8s",
        "tmpfs": ["/tmp:rw,noexec,nosuid,size=64m"],
    }


def test_preserves_identity_security_mounts_and_singleton():
    spec = backend.service_spec(
        "tradix-canary", "worker-developer", source(), "node-id", {"developer-plane": "overlay-id"}
    )
    task = spec["TaskTemplate"]
    assert spec["Mode"] == {"Replicated": {"Replicas": 1}}
    assert spec["UpdateConfig"]["Order"] == "stop-first"
    assert task["Placement"]["Constraints"] == ["node.id==node-id"]
    assert task["ContainerSpec"]["Privileges"]["NoNewPrivileges"] is True
    assert task["ContainerSpec"]["Mounts"][1]["ReadOnly"] is True
    assert ["noexec"] in task["ContainerSpec"]["Mounts"][2]["TmpfsOptions"]["Options"]
    assert task["ContainerSpec"]["StopGracePeriod"] == 8_000_000_000
    assert task["Networks"] == [{"Target": "overlay-id", "Aliases": ["worker-developer"]}]


@pytest.mark.parametrize(
    "change",
    [
        {"gpus": "all"},
        {"deploy": {"replicas": 2}},
        {"privileged": True},
        {"image": "ghcr.io/example:latest"},
        {"cap_add": ["SYS_ADMIN"]},
        {"devices": ["/dev/nvidia0"]},
        {"command": "sh -c bad"},
        {"security_opt": ["seccomp:unconfined"]},
        {"volumes": [{"type": "bind", "source": "/var/run/docker.sock", "target": "/socket"}]},
    ],
)
def test_rejects_silent_degradation(change):
    with pytest.raises(ValueError):
        backend.service_spec(
            "tradix-canary",
            "worker-developer",
            source() | change,
            "node",
            {"developer-plane": "id"},
        )


def test_rejects_missing_node_or_role_network():
    with pytest.raises(ValueError):
        backend.service_spec("tradix-canary", "worker-developer", source(), "", {})
    with pytest.raises(ValueError):
        backend.service_spec("tradix-canary", "worker-developer", source(), "node", {})


def test_cannot_adopt_foreign_service():
    instance = backend.SwarmBackend("tradix-canary")
    instance.request = lambda *args, **kwargs: [
        {"Spec": {"Name": "tradix-canary_worker-developer", "Labels": {}}}
    ]
    with pytest.raises(ValueError, match="propiedad"):
        instance.owned("worker-developer")


def test_rollback_preserves_service_and_data():
    instance = backend.SwarmBackend("tradix-canary")
    existing = {
        "ID": "id",
        "Version": {"Index": 4},
        "Spec": backend.service_spec(
            "tradix-canary", "worker-developer", source(), "node", {"developer-plane": "id"}
        ),
    }
    instance.owned = lambda name: existing
    calls = []
    instance.request = lambda *args, **kwargs: calls.append((args, kwargs))
    instance.rollback(["worker-developer"])
    assert calls[0][0] == ("POST", "/services/id/update")
    assert calls[0][1]["json"]["Mode"]["Replicated"]["Replicas"] == 0
    assert calls[0][1]["json"]["TaskTemplate"] == existing["Spec"]["TaskTemplate"]
