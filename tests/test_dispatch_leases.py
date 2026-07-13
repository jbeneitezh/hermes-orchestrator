from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.models import Base, Run, Task
from hermes_orchestrator.task_services import (
    LifecycleError,
    claim_dispatching_runs,
    heartbeat_run_lease,
    release_run_lease,
)

NOW = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
LEASE = timedelta(seconds=30)


@pytest.fixture
def session_factory(tmp_path: Path) -> Iterator[sessionmaker[Session]]:
    engine = create_engine(f"sqlite+pysqlite:///{(tmp_path / 'leases.db').as_posix()}")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


def add_run(
    factory: sessionmaker[Session],
    *,
    status: str = "dispatching",
    next_attempt_at: datetime = NOW,
) -> uuid.UUID:
    suffix = uuid.uuid4().hex
    with factory() as session:
        task = Task(
            requester_actor_id="agent:leader",
            assignee_actor_id="agent:developer",
            reviewer_actor_id=None,
            idempotency_key=f"task-{suffix}",
            request_hash=suffix,
            objective="Validar lease durable",
            acceptance_criteria=["Claim exclusivo"],
            independent_review=False,
            status="dispatched",
        )
        session.add(task)
        session.flush()
        run = Run(
            task_id=task.id,
            operation_id=task.operation_id,
            attempt_number=1,
            worker_actor_id="agent:developer",
            requested_profile_id="spark-low",
            dispatch_idempotency_key=f"dispatch-{suffix}",
            dispatch_hash=suffix,
            status=status,
            next_attempt_at=next_attempt_at,
            timeout_at=NOW + timedelta(minutes=15),
        )
        session.add(run)
        session.commit()
        return run.id


def test_claim_records_owner_attempt_and_heartbeat(session_factory) -> None:
    run_id = add_run(session_factory)

    with session_factory() as session:
        claimed = claim_dispatching_runs(
            session,
            lease_owner="dispatcher:a",
            lease_duration=LEASE,
            now=NOW,
        )

    assert [run.id for run in claimed] == [run_id]
    assert claimed[0].lease_owner == "dispatcher:a"
    assert claimed[0].dispatch_attempts == 1
    assert claimed[0].lease_acquired_at == NOW
    assert claimed[0].heartbeat_at == NOW
    assert claimed[0].lease_expires_at == NOW + LEASE


def test_second_claimer_cannot_take_a_current_lease(session_factory) -> None:
    run_id = add_run(session_factory)
    with session_factory() as first:
        assert (
            claim_dispatching_runs(
                first,
                lease_owner="dispatcher:a",
                lease_duration=LEASE,
                now=NOW,
            )[0].id
            == run_id
        )
    with session_factory() as second:
        assert (
            claim_dispatching_runs(
                second,
                lease_owner="dispatcher:b",
                lease_duration=LEASE,
                now=NOW + timedelta(seconds=10),
            )
            == []
        )


def test_heartbeat_extends_only_the_current_owners_lease(session_factory) -> None:
    run_id = add_run(session_factory)
    with session_factory() as session:
        claim_dispatching_runs(
            session,
            lease_owner="dispatcher:a",
            lease_duration=LEASE,
            now=NOW,
        )
        renewed = heartbeat_run_lease(
            session,
            run_id=run_id,
            lease_owner="dispatcher:a",
            lease_duration=LEASE,
            now=NOW + timedelta(seconds=20),
        )
        with pytest.raises(LifecycleError) as captured:
            heartbeat_run_lease(
                session,
                run_id=run_id,
                lease_owner="dispatcher:b",
                lease_duration=LEASE,
                now=NOW + timedelta(seconds=21),
            )

    assert renewed.heartbeat_at == NOW + timedelta(seconds=20)
    assert renewed.lease_expires_at == NOW + timedelta(seconds=50)
    assert captured.value.code == "dispatch_lease_owner_mismatch"


def test_expired_lease_is_recoverable_by_another_claimer(session_factory) -> None:
    run_id = add_run(session_factory)
    with session_factory() as first:
        claim_dispatching_runs(
            first,
            lease_owner="dispatcher:a",
            lease_duration=LEASE,
            now=NOW,
        )
    with session_factory() as second:
        reclaimed = claim_dispatching_runs(
            second,
            lease_owner="dispatcher:b",
            lease_duration=LEASE,
            now=NOW + timedelta(seconds=31),
        )

    assert [run.id for run in reclaimed] == [run_id]
    assert reclaimed[0].lease_owner == "dispatcher:b"
    assert reclaimed[0].dispatch_attempts == 2


def test_terminal_run_is_never_claimed(session_factory) -> None:
    add_run(session_factory, status="completed")
    with session_factory() as session:
        assert (
            claim_dispatching_runs(
                session,
                lease_owner="dispatcher:a",
                lease_duration=LEASE,
                now=NOW,
            )
            == []
        )


def test_future_attempt_waits_until_due_and_release_reschedules(session_factory) -> None:
    future = NOW + timedelta(minutes=1)
    run_id = add_run(session_factory, next_attempt_at=future)
    with session_factory() as session:
        assert (
            claim_dispatching_runs(
                session,
                lease_owner="dispatcher:a",
                lease_duration=LEASE,
                now=NOW,
            )
            == []
        )
        claimed = claim_dispatching_runs(
            session,
            lease_owner="dispatcher:a",
            lease_duration=LEASE,
            now=future,
        )
        released = release_run_lease(
            session,
            run_id=run_id,
            lease_owner="dispatcher:a",
            next_attempt_at=future + timedelta(minutes=1),
            now=future + timedelta(seconds=1),
        )
        not_due = claim_dispatching_runs(
            session,
            lease_owner="dispatcher:b",
            lease_duration=LEASE,
            now=future + timedelta(seconds=30),
        )

    assert [run.id for run in claimed] == [run_id]
    assert released.lease_owner is None
    assert released.next_attempt_at == future + timedelta(minutes=1)
    assert not_due == []


def test_invalid_or_stale_lease_commands_fail_closed(session_factory) -> None:
    run_id = add_run(session_factory)
    with session_factory() as session:
        with pytest.raises(LifecycleError) as invalid_owner:
            claim_dispatching_runs(
                session,
                lease_owner=" ",
                lease_duration=LEASE,
                now=NOW,
            )
        with pytest.raises(LifecycleError) as invalid_limit:
            claim_dispatching_runs(
                session,
                lease_owner="dispatcher:a",
                lease_duration=LEASE,
                limit=0,
                now=NOW,
            )
        with pytest.raises(LifecycleError) as invalid_duration:
            claim_dispatching_runs(
                session,
                lease_owner="dispatcher:a",
                lease_duration=timedelta(0),
                now=NOW,
            )
        with pytest.raises(LifecycleError) as missing_run:
            heartbeat_run_lease(
                session,
                run_id=uuid.uuid4(),
                lease_owner="dispatcher:a",
                lease_duration=LEASE,
                now=NOW,
            )
        claim_dispatching_runs(
            session,
            lease_owner="dispatcher:a",
            lease_duration=LEASE,
            now=NOW,
        )
        with pytest.raises(LifecycleError) as expired:
            heartbeat_run_lease(
                session,
                run_id=run_id,
                lease_owner="dispatcher:a",
                lease_duration=LEASE,
                now=NOW + timedelta(seconds=31),
            )
        run = session.get(Run, run_id)
        assert run is not None
        run.status = "completed"
        run.lease_expires_at = NOW + timedelta(minutes=2)
        session.commit()
        with pytest.raises(LifecycleError) as inactive:
            heartbeat_run_lease(
                session,
                run_id=run_id,
                lease_owner="dispatcher:a",
                lease_duration=LEASE,
                now=NOW + timedelta(seconds=32),
            )

    assert invalid_owner.value.code == "invalid_lease_owner"
    assert invalid_limit.value.code == "invalid_claim_limit"
    assert invalid_duration.value.code == "invalid_lease_duration"
    assert missing_run.value.code == "not_found"
    assert expired.value.code == "dispatch_lease_expired"
    assert inactive.value.code == "dispatch_lease_inactive"
