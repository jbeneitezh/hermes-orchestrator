from __future__ import annotations

from typing import cast

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def create_database_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True)


def database_is_ready(engine: Engine) -> bool:
    with engine.connect() as connection:
        result = cast(int, connection.execute(text("SELECT 1")).scalar_one())
        return result == 1
