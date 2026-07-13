from __future__ import annotations

from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.models import Agent

TEST_TOKEN_PREFIX = "test-only-internal-token"


def token_for(actor_id: str) -> str:
    return f"{TEST_TOKEN_PREFIX}-{actor_id.replace(':', '-')}"


def token_settings(*actor_ids: str) -> dict[str, dict[str, SecretStr]]:
    tokens = {actor_id: SecretStr(token_for(actor_id)) for actor_id in actor_ids}
    return {"internal_auth_tokens": tokens}


def auth_headers(actor_id: str, **extra: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token_for(actor_id)}",
        "X-Actor-Id": actor_id,
        **extra,
    }


def seed_active_auth_agents(factory: sessionmaker[Session], settings: Settings) -> None:
    with factory() as session:
        for actor_id in settings.internal_auth_tokens:
            if not actor_id.startswith("agent:"):
                continue
            slug = actor_id.removeprefix("agent:")
            role = settings.actor_roles.get(actor_id)
            if role is None:
                continue
            agent = session.scalar(select(Agent).where(Agent.slug == slug))
            if agent is None:
                agent = Agent(
                    slug=slug,
                    role=role,
                    description=f"Fixture de autenticación {slug}",
                    owner_actor_id="user:owner",
                )
                session.add(agent)
            agent.desired_state = "active"
        session.commit()
