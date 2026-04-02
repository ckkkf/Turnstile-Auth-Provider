import json
import logging
import os
from typing import Any, Dict, Optional, Union

import aiosqlite
import asyncpg

DEFAULT_SQLITE_PATH = "results.db"
DEFAULT_DB_TYPE = "pgsql"

DB_CONFIG = {
    "type": os.getenv("DB_TYPE", DEFAULT_DB_TYPE).lower(),
    "sqlite_path": os.getenv("DB_PATH", DEFAULT_SQLITE_PATH),
    "pgsql_dsn": os.getenv("DATABASE_URL") or os.getenv("PGSQL_DSN"),
    "pgsql_host": os.getenv("PGSQL_HOST", "127.0.0.1"),
    "pgsql_port": int(os.getenv("PGSQL_PORT", "5432")),
    "pgsql_user": os.getenv("PGSQL_USER", "postgres"),
    "pgsql_password": os.getenv("PGSQL_PASSWORD", "123456"),
    "pgsql_database": os.getenv("PGSQL_DATABASE", "Turnstile-Auth-Provider"),
}

PRAGMA_SETTINGS = [
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA cache_size=10000",
    "PRAGMA temp_store=MEMORY",
    "PRAGMA busy_timeout=30000",
]


def configure_database(
    db_type: Optional[str] = None,
    db_path: Optional[str] = None,
    db_url: Optional[str] = None,
) -> None:
    if db_type:
        DB_CONFIG["type"] = db_type.lower()
    if db_path:
        DB_CONFIG["sqlite_path"] = db_path
    if db_url:
        DB_CONFIG["pgsql_dsn"] = db_url


def _logger() -> logging.Logger:
    return logging.getLogger("TurnstileAPIServer")


def _get_db_type() -> str:
    db_type = DB_CONFIG["type"]
    if db_type not in {"sqlite", "pgsql", "postgres", "postgresql"}:
        raise ValueError(f"Unsupported database type: {db_type}")
    return "pgsql" if db_type in {"pgsql", "postgres", "postgresql"} else "sqlite"


def _get_pgsql_connect_kwargs() -> Dict[str, Any]:
    if DB_CONFIG["pgsql_dsn"]:
        return {"dsn": DB_CONFIG["pgsql_dsn"]}

    return {
        "host": DB_CONFIG["pgsql_host"],
        "port": DB_CONFIG["pgsql_port"],
        "user": DB_CONFIG["pgsql_user"],
        "password": DB_CONFIG["pgsql_password"],
        "database": DB_CONFIG["pgsql_database"],
    }


async def _apply_pragma_settings(db: aiosqlite.Connection) -> None:
    for pragma in PRAGMA_SETTINGS:
        await db.execute(pragma)


async def _sqlite_connect() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_CONFIG["sqlite_path"])
    await _apply_pragma_settings(db)
    return db


async def _pgsql_connect() -> asyncpg.Connection:
    return await asyncpg.connect(**_get_pgsql_connect_kwargs())


def _serialize_data(data: Union[Dict[str, Any], str]) -> str:
    return json.dumps(data) if isinstance(data, dict) else data


def _deserialize_data(data: str) -> Union[Dict[str, Any], str]:
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return data


async def init_db() -> None:
    db_type = _get_db_type()
    try:
        if db_type == "sqlite":
            async with await _sqlite_connect() as db:
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS results (
                        task_id TEXT PRIMARY KEY,
                        type TEXT NOT NULL,
                        data TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                await db.commit()
                _logger().info(f"SQLite database initialized: {DB_CONFIG['sqlite_path']}")
                return

        conn = await _pgsql_connect()
        try:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS results (
                    task_id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            _logger().info("PostgreSQL database initialized")
        finally:
            await conn.close()
    except Exception as e:
        _logger().error(f"Database initialization error: {e}")
        raise


async def save_result(task_id: str, task_type: str, data: Union[Dict[str, Any], str]) -> None:
    db_type = _get_db_type()
    data_json = _serialize_data(data)
    try:
        if db_type == "sqlite":
            async with await _sqlite_connect() as db:
                await db.execute(
                    "REPLACE INTO results (task_id, type, data) VALUES (?, ?, ?)",
                    (task_id, task_type, data_json),
                )
                await db.commit()
                return

        conn = await _pgsql_connect()
        try:
            await conn.execute(
                """
                INSERT INTO results (task_id, type, data)
                VALUES ($1, $2, $3)
                ON CONFLICT (task_id)
                DO UPDATE SET type = EXCLUDED.type, data = EXCLUDED.data
                """,
                task_id,
                task_type,
                data_json,
            )
        finally:
            await conn.close()
    except Exception as e:
        _logger().error(f"Error saving result {task_id}: {e}")
        raise


async def load_result(task_id: str) -> Optional[Union[Dict[str, Any], str]]:
    db_type = _get_db_type()
    try:
        if db_type == "sqlite":
            async with await _sqlite_connect() as db:
                async with db.execute("SELECT data FROM results WHERE task_id = ?", (task_id,)) as cursor:
                    row = await cursor.fetchone()
                    return _deserialize_data(row[0]) if row else None

        conn = await _pgsql_connect()
        try:
            row = await conn.fetchrow("SELECT data FROM results WHERE task_id = $1", task_id)
            return _deserialize_data(row["data"]) if row else None
        finally:
            await conn.close()
    except Exception as e:
        _logger().error(f"Error loading result {task_id}: {e}")
        return None


async def load_all_results() -> Dict[str, Any]:
    db_type = _get_db_type()
    try:
        if db_type == "sqlite":
            async with await _sqlite_connect() as db:
                results: Dict[str, Any] = {}
                async with db.execute("SELECT task_id, data FROM results") as cursor:
                    async for row in cursor:
                        results[row[0]] = _deserialize_data(row[1])
                return results

        conn = await _pgsql_connect()
        try:
            rows = await conn.fetch("SELECT task_id, data FROM results")
            return {row["task_id"]: _deserialize_data(row["data"]) for row in rows}
        finally:
            await conn.close()
    except Exception as e:
        _logger().error(f"Error loading all results: {e}")
        return {}


async def delete_result(task_id: str) -> None:
    db_type = _get_db_type()
    try:
        if db_type == "sqlite":
            async with await _sqlite_connect() as db:
                await db.execute("DELETE FROM results WHERE task_id = ?", (task_id,))
                await db.commit()
                return

        conn = await _pgsql_connect()
        try:
            await conn.execute("DELETE FROM results WHERE task_id = $1", task_id)
        finally:
            await conn.close()
    except Exception as e:
        _logger().error(f"Error deleting result {task_id}: {e}")


async def get_pending_count() -> int:
    db_type = _get_db_type()
    try:
        if db_type == "sqlite":
            async with await _sqlite_connect() as db:
                async with db.execute("SELECT COUNT(*) FROM results WHERE data LIKE '%CAPTCHA_NOT_READY%'") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0

        conn = await _pgsql_connect()
        try:
            value = await conn.fetchval("SELECT COUNT(*) FROM results WHERE data LIKE '%CAPTCHA_NOT_READY%'")
            return int(value or 0)
        finally:
            await conn.close()
    except Exception as e:
        _logger().error(f"Error getting pending count: {e}")
        return 0


async def cleanup_old_results(days_old: int = 1) -> int:
    db_type = _get_db_type()
    try:
        if db_type == "sqlite":
            async with await _sqlite_connect() as db:
                cursor = await db.execute(
                    "DELETE FROM results WHERE created_at < datetime('now', ?) ",
                    (f"-{days_old} days",),
                )
                deleted_count = cursor.rowcount or 0
                await db.commit()
                _logger().info(f"Cleaned up {deleted_count} old results")
                return deleted_count

        conn = await _pgsql_connect()
        try:
            deleted_count = await conn.fetchval(
                """
                WITH deleted AS (
                    DELETE FROM results
                    WHERE created_at < CURRENT_TIMESTAMP - ($1::text || ' days')::interval
                    RETURNING 1
                )
                SELECT COUNT(*) FROM deleted
                """,
                str(days_old),
            )
            deleted_count = int(deleted_count or 0)
            _logger().info(f"Cleaned up {deleted_count} old results")
            return deleted_count
        finally:
            await conn.close()
    except Exception as e:
        _logger().error(f"Error cleaning up old results: {e}")
        return 0
