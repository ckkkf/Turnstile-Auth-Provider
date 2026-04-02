import json
import logging
import os
import secrets
import uuid
from typing import Any, Dict, List, Optional, Union

import aiosqlite
import asyncpg
from werkzeug.security import check_password_hash, generate_password_hash

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


def get_database_config() -> Dict[str, Any]:
    return dict(DB_CONFIG)


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
    db.row_factory = aiosqlite.Row
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


def _dict_from_sqlite_row(row: aiosqlite.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _generate_id() -> str:
    return str(uuid.uuid4())


def _normalize_task_result(task_row: Dict[str, Any]) -> Dict[str, Any]:
    raw_data = task_row.get("data", "")
    parsed = _deserialize_data(raw_data) if isinstance(raw_data, str) else raw_data
    status = "ready"
    value = None
    elapsed_time = None

    if isinstance(parsed, dict):
        if parsed.get("status") == "CAPTCHA_NOT_READY":
            status = "processing"
        elif parsed.get("value") == "CAPTCHA_FAIL":
            status = "failed"
            value = parsed.get("value")
        elif parsed.get("value"):
            status = "ready"
            value = parsed.get("value")
        elapsed_time = parsed.get("elapsed_time")
    elif parsed == "CAPTCHA_NOT_READY":
        status = "processing"
    elif parsed == "CAPTCHA_FAIL":
        status = "failed"
        value = parsed

    return {
        "task_id": task_row.get("task_id"),
        "type": task_row.get("type"),
        "status": status,
        "value": value,
        "elapsed_time": elapsed_time,
        "created_at": str(task_row.get("created_at")),
        "data": parsed,
    }


def _parse_elapsed_seconds(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    try:
        return float(text)
    except ValueError:
        pass

    lowered = text.lower().replace("秒", "s")
    if lowered.endswith("ms"):
        try:
            return float(lowered[:-2].strip()) / 1000
        except ValueError:
            return None
    if lowered.endswith("s"):
        try:
            return float(lowered[:-1].strip())
        except ValueError:
            return None

    parts = text.split(":")
    if len(parts) == 3:
        try:
            hours = float(parts[0])
            minutes = float(parts[1])
            seconds = float(parts[2])
            return hours * 3600 + minutes * 60 + seconds
        except ValueError:
            return None

    return None


def _format_elapsed_seconds(value: float | None) -> str:
    if value is None:
        return "-"
    if value < 1:
        return f"{value * 1000:.0f} ms"
    if value < 60:
        return f"{value:.2f} s"

    minutes = int(value // 60)
    seconds = value % 60
    if minutes < 60:
        return f"{minutes:02d}:{seconds:05.2f}"

    hours = int(minutes // 60)
    minutes = minutes % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:05.2f}"


async def _init_sqlite_schema(db: aiosqlite.Connection) -> None:
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
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS portal_users (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            plan TEXT NOT NULL DEFAULT 'free',
            points INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS portal_admins (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            role TEXT NOT NULL DEFAULT 'operator',
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS points_transactions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            amount INTEGER NOT NULL,
            action TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS service_api_keys (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            name TEXT NOT NULL,
            token TEXT NOT NULL,
            scopes TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            expires_at TEXT,
            last_used_at TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS service_webhooks (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            secret TEXT,
            events TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'enabled',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS service_ip_whitelist (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            ip_address TEXT NOT NULL,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS service_billing_orders (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            amount REAL NOT NULL,
            points INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'paid',
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.commit()


async def _init_pgsql_schema(conn: asyncpg.Connection) -> None:
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
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portal_users (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            plan TEXT NOT NULL DEFAULT 'free',
            points INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portal_admins (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            role TEXT NOT NULL DEFAULT 'operator',
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS points_transactions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            amount INTEGER NOT NULL,
            action TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS service_api_keys (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            name TEXT NOT NULL,
            token TEXT NOT NULL,
            scopes TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            expires_at TEXT,
            last_used_at TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS service_webhooks (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            secret TEXT,
            events TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'enabled',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS service_ip_whitelist (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            ip_address TEXT NOT NULL,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS service_billing_orders (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            amount DOUBLE PRECISION NOT NULL,
            points INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'paid',
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


async def _ensure_sqlite_migrations(db: aiosqlite.Connection) -> None:
    async with db.execute("PRAGMA table_info(portal_users)") as cursor:
        rows = await cursor.fetchall()
        user_columns = {row[1] for row in rows}
    if "password_hash" not in user_columns:
        await db.execute("ALTER TABLE portal_users ADD COLUMN password_hash TEXT")

    async with db.execute("PRAGMA table_info(portal_admins)") as cursor:
        rows = await cursor.fetchall()
        columns = {row[1] for row in rows}
    if "password_hash" not in columns:
        await db.execute("ALTER TABLE portal_admins ADD COLUMN password_hash TEXT")
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS service_api_keys (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            name TEXT NOT NULL,
            token TEXT NOT NULL,
            scopes TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            expires_at TEXT,
            last_used_at TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS service_webhooks (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            secret TEXT,
            events TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'enabled',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS service_ip_whitelist (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            ip_address TEXT NOT NULL,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS service_billing_orders (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            amount REAL NOT NULL,
            points INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'paid',
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await db.commit()


async def _ensure_pgsql_migrations(conn: asyncpg.Connection) -> None:
    user_exists = await conn.fetchval(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'portal_users' AND column_name = 'password_hash'
        """
    )
    if not user_exists:
        await conn.execute("ALTER TABLE portal_users ADD COLUMN password_hash TEXT")

    exists = await conn.fetchval(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'portal_admins' AND column_name = 'password_hash'
        """
    )
    if not exists:
        await conn.execute("ALTER TABLE portal_admins ADD COLUMN password_hash TEXT")
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS points_transactions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            amount INTEGER NOT NULL,
            action TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS service_api_keys (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            name TEXT NOT NULL,
            token TEXT NOT NULL,
            scopes TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            expires_at TEXT,
            last_used_at TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS service_webhooks (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            secret TEXT,
            events TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'enabled',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS service_ip_whitelist (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            ip_address TEXT NOT NULL,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS service_billing_orders (
            id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            amount DOUBLE PRECISION NOT NULL,
            points INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'paid',
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


async def _seed_sqlite_data(db: aiosqlite.Connection) -> None:
    async with db.execute("SELECT COUNT(*) AS total FROM portal_admins") as cursor:
        admin_total = (await cursor.fetchone())["total"]
    if admin_total == 0:
        await db.execute(
            "INSERT INTO portal_admins (id, username, password_hash, role, status) VALUES (?, ?, ?, ?, ?)",
            (_generate_id(), "admin", generate_password_hash("admin123456"), "super_admin", "active"),
        )
    else:
        await db.execute(
            "UPDATE portal_admins SET password_hash = ? WHERE username = ? AND (password_hash IS NULL OR password_hash = '')",
            (generate_password_hash("admin123456"), "admin"),
        )

    async with db.execute("SELECT COUNT(*) AS total FROM portal_users") as cursor:
        user_total = (await cursor.fetchone())["total"]
    if user_total == 0:
        user_id = _generate_id()
        await db.execute(
            "INSERT INTO portal_users (id, username, email, password_hash, plan, points, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, "demo_user", "demo@example.com", generate_password_hash("demo123456"), "starter", 100, "active"),
        )
        await db.execute(
            "INSERT INTO points_transactions (id, user_id, amount, action, description) VALUES (?, ?, ?, ?, ?)",
            (_generate_id(), user_id, 100, "seed", "Initial starter points"),
        )
    await db.execute(
        "UPDATE portal_users SET password_hash = ? WHERE username = ? AND (password_hash IS NULL OR password_hash = '')",
        (generate_password_hash("demo123456"), "demo_user"),
    )
    async with db.execute("SELECT id FROM portal_users WHERE username = ? LIMIT 1", ("demo_user",)) as cursor:
        demo_user = await cursor.fetchone()
    if demo_user:
        demo_user_id = demo_user["id"]
        async with db.execute("SELECT COUNT(*) AS total FROM service_api_keys WHERE owner_id = ? AND owner_kind = ?", (demo_user_id, "user")) as cursor:
            service_total = (await cursor.fetchone())["total"]
        if int(service_total or 0) == 0:
            await db.execute(
                "INSERT INTO service_api_keys (id, owner_id, owner_kind, name, token, scopes, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (_generate_id(), demo_user_id, "user", "默认接入 Key", f"ts_{secrets.token_urlsafe(18)}", "solve,docs", "2099-12-31"),
            )
            await db.execute(
                "INSERT INTO service_webhooks (id, owner_id, owner_kind, endpoint, secret, events) VALUES (?, ?, ?, ?, ?, ?)",
                (_generate_id(), demo_user_id, "user", "https://example.com/webhook", secrets.token_hex(8), "task.ready,task.failed"),
            )
            await db.execute(
                "INSERT INTO service_ip_whitelist (id, owner_id, owner_kind, ip_address, note) VALUES (?, ?, ?, ?, ?)",
                (_generate_id(), demo_user_id, "user", "127.0.0.1", "本地调试"),
            )
            await db.execute(
                "INSERT INTO service_billing_orders (id, owner_id, owner_kind, amount, points, status, description) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (_generate_id(), demo_user_id, "user", 29.9, 300, "paid", "Starter 套餐充值"),
            )
    await db.commit()


async def _seed_pgsql_data(conn: asyncpg.Connection) -> None:
    admin_total = await conn.fetchval("SELECT COUNT(*) FROM portal_admins")
    if int(admin_total or 0) == 0:
        await conn.execute(
            "INSERT INTO portal_admins (id, username, password_hash, role, status) VALUES ($1, $2, $3, $4, $5)",
            _generate_id(),
            "admin",
            generate_password_hash("admin123456"),
            "super_admin",
            "active",
        )
    else:
        await conn.execute(
            "UPDATE portal_admins SET password_hash = $1 WHERE username = $2 AND (password_hash IS NULL OR password_hash = '')",
            generate_password_hash("admin123456"),
            "admin",
        )

    user_total = await conn.fetchval("SELECT COUNT(*) FROM portal_users")
    if int(user_total or 0) == 0:
        user_id = _generate_id()
        await conn.execute(
            "INSERT INTO portal_users (id, username, email, password_hash, plan, points, status) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            user_id,
            "demo_user",
            "demo@example.com",
            generate_password_hash("demo123456"),
            "starter",
            100,
            "active",
        )
        await conn.execute(
            "INSERT INTO points_transactions (id, user_id, amount, action, description) VALUES ($1, $2, $3, $4, $5)",
            _generate_id(),
            user_id,
            100,
            "seed",
            "Initial starter points",
        )
    else:
        user_id = await conn.fetchval("SELECT id FROM portal_users WHERE username = $1 LIMIT 1", "demo_user")

    if user_id:
        service_total = await conn.fetchval(
            "SELECT COUNT(*) FROM service_api_keys WHERE owner_id = $1 AND owner_kind = $2",
            user_id,
            "user",
        )
        if int(service_total or 0) == 0:
            await conn.execute(
                "INSERT INTO service_api_keys (id, owner_id, owner_kind, name, token, scopes, expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                _generate_id(), user_id, "user", "默认接入 Key", f"ts_{secrets.token_urlsafe(18)}", "solve,docs", "2099-12-31",
            )
            await conn.execute(
                "INSERT INTO service_webhooks (id, owner_id, owner_kind, endpoint, secret, events) VALUES ($1, $2, $3, $4, $5, $6)",
                _generate_id(), user_id, "user", "https://example.com/webhook", secrets.token_hex(8), "task.ready,task.failed",
            )
            await conn.execute(
                "INSERT INTO service_ip_whitelist (id, owner_id, owner_kind, ip_address, note) VALUES ($1, $2, $3, $4, $5)",
                _generate_id(), user_id, "user", "127.0.0.1", "本地调试",
            )
            await conn.execute(
                "INSERT INTO service_billing_orders (id, owner_id, owner_kind, amount, points, status, description) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                _generate_id(), user_id, "user", 29.9, 300, "paid", "Starter 套餐充值",
            )


async def init_db() -> None:
    db_type = _get_db_type()
    try:
        if db_type == "sqlite":
            async with await _sqlite_connect() as db:
                await _init_sqlite_schema(db)
                await _ensure_sqlite_migrations(db)
                await _seed_sqlite_data(db)
                _logger().info(f"SQLite database initialized: {DB_CONFIG['sqlite_path']}")
                return

        conn = await _pgsql_connect()
        try:
            await _init_pgsql_schema(conn)
            await _ensure_pgsql_migrations(conn)
            await _seed_pgsql_data(conn)
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
                    return _deserialize_data(row["data"]) if row else None

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
                        results[row["task_id"]] = _deserialize_data(row["data"])
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
    stats = await get_portal_stats()
    return int(stats["tasks_pending"])


async def cleanup_old_results(days_old: int = 1) -> int:
    db_type = _get_db_type()
    try:
        if db_type == "sqlite":
            async with await _sqlite_connect() as db:
                cursor = await db.execute(
                    "DELETE FROM results WHERE created_at < datetime('now', ?)",
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


async def list_portal_users() -> List[Dict[str, Any]]:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            async with db.execute("SELECT * FROM portal_users ORDER BY created_at DESC") as cursor:
                rows = await cursor.fetchall()
                return [_dict_from_sqlite_row(row) for row in rows]

    conn = await _pgsql_connect()
    try:
        rows = await conn.fetch("SELECT * FROM portal_users ORDER BY created_at DESC")
        return [dict(row) for row in rows]
    finally:
        await conn.close()


async def create_portal_user(username: str, email: str, plan: str = "free", points: int = 0, password: str = "demo123456") -> None:
    user_id = _generate_id()
    password_hash = generate_password_hash(password)
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute(
                "INSERT INTO portal_users (id, username, email, password_hash, plan, points, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (user_id, username, email, password_hash, plan, points, "active"),
            )
            if points:
                await db.execute(
                    "INSERT INTO points_transactions (id, user_id, amount, action, description) VALUES (?, ?, ?, ?, ?)",
                    (_generate_id(), user_id, points, "create_user", "Initial points on user creation"),
                )
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute(
            "INSERT INTO portal_users (id, username, email, password_hash, plan, points, status) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            user_id,
            username,
            email,
            password_hash,
            plan,
            points,
            "active",
        )
        if points:
            await conn.execute(
                "INSERT INTO points_transactions (id, user_id, amount, action, description) VALUES ($1, $2, $3, $4, $5)",
                _generate_id(),
                user_id,
                points,
                "create_user",
                "Initial points on user creation",
            )
    finally:
        await conn.close()


async def update_portal_user_status(user_id: str, status: str) -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute("UPDATE portal_users SET status = ? WHERE id = ?", (status, user_id))
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute("UPDATE portal_users SET status = $1 WHERE id = $2", status, user_id)
    finally:
        await conn.close()


async def update_portal_user(
    user_id: str,
    username: str,
    email: str,
    plan: str,
    status: str,
) -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute(
                "UPDATE portal_users SET username = ?, email = ?, plan = ?, status = ? WHERE id = ?",
                (username, email, plan, status, user_id),
            )
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute(
            "UPDATE portal_users SET username = $1, email = $2, plan = $3, status = $4 WHERE id = $5",
            username,
            email,
            plan,
            status,
            user_id,
        )
    finally:
        await conn.close()


async def delete_portal_user(user_id: str) -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute("DELETE FROM points_transactions WHERE user_id = ?", (user_id,))
            await db.execute("DELETE FROM portal_users WHERE id = ?", (user_id,))
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute("DELETE FROM points_transactions WHERE user_id = $1", user_id)
        await conn.execute("DELETE FROM portal_users WHERE id = $1", user_id)
    finally:
        await conn.close()


async def adjust_portal_user_points(user_id: str, amount: int, action: str = "manual_adjust", description: str = "") -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute("UPDATE portal_users SET points = points + ? WHERE id = ?", (amount, user_id))
            await db.execute(
                "INSERT INTO points_transactions (id, user_id, amount, action, description) VALUES (?, ?, ?, ?, ?)",
                (_generate_id(), user_id, amount, action, description or "Points adjusted by admin"),
            )
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute("UPDATE portal_users SET points = points + $1 WHERE id = $2", amount, user_id)
        await conn.execute(
            "INSERT INTO points_transactions (id, user_id, amount, action, description) VALUES ($1, $2, $3, $4, $5)",
            _generate_id(),
            user_id,
            amount,
            action,
            description or "Points adjusted by admin",
        )
    finally:
        await conn.close()


async def authenticate_portal_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            async with db.execute(
                "SELECT * FROM portal_users WHERE username = ? AND status = 'active'",
                (username,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                user = _dict_from_sqlite_row(row)
                if user.get("password_hash") and check_password_hash(user["password_hash"], password):
                    user.pop("password_hash", None)
                    return user
                return None

    conn = await _pgsql_connect()
    try:
        row = await conn.fetchrow(
            "SELECT * FROM portal_users WHERE username = $1 AND status = 'active'",
            username,
        )
        if not row:
            return None
        user = dict(row)
        if user.get("password_hash") and check_password_hash(user["password_hash"], password):
            user.pop("password_hash", None)
            return user
        return None
    finally:
        await conn.close()


async def update_portal_user_password(user_id: str, new_password: str) -> None:
    password_hash = generate_password_hash(new_password)
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute("UPDATE portal_users SET password_hash = ? WHERE id = ?", (password_hash, user_id))
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute("UPDATE portal_users SET password_hash = $1 WHERE id = $2", password_hash, user_id)
    finally:
        await conn.close()


async def update_portal_admin_password(admin_id: str, new_password: str) -> None:
    password_hash = generate_password_hash(new_password)
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute("UPDATE portal_admins SET password_hash = ? WHERE id = ?", (password_hash, admin_id))
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute("UPDATE portal_admins SET password_hash = $1 WHERE id = $2", password_hash, admin_id)
    finally:
        await conn.close()


async def list_portal_admins() -> List[Dict[str, Any]]:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            async with db.execute("SELECT * FROM portal_admins ORDER BY created_at DESC") as cursor:
                rows = await cursor.fetchall()
                return [_dict_from_sqlite_row(row) for row in rows]

    conn = await _pgsql_connect()
    try:
        rows = await conn.fetch("SELECT * FROM portal_admins ORDER BY created_at DESC")
        return [dict(row) for row in rows]
    finally:
        await conn.close()


async def create_portal_admin(username: str, password: str = "admin123456", role: str = "operator") -> None:
    admin_id = _generate_id()
    password_hash = generate_password_hash(password)
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute(
                "INSERT INTO portal_admins (id, username, password_hash, role, status) VALUES (?, ?, ?, ?, ?)",
                (admin_id, username, password_hash, role, "active"),
            )
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute(
            "INSERT INTO portal_admins (id, username, password_hash, role, status) VALUES ($1, $2, $3, $4, $5)",
            admin_id,
            username,
            password_hash,
            role,
            "active",
        )
    finally:
        await conn.close()


async def update_portal_admin(admin_id: str, username: str, role: str, status: str) -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute(
                "UPDATE portal_admins SET username = ?, role = ?, status = ? WHERE id = ?",
                (username, role, status, admin_id),
            )
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute(
            "UPDATE portal_admins SET username = $1, role = $2, status = $3 WHERE id = $4",
            username,
            role,
            status,
            admin_id,
        )
    finally:
        await conn.close()


async def promote_user_to_admin(user_id: str, role: str = "operator", password: str = "admin123456") -> None:
    users = await list_portal_users()
    user = next((item for item in users if item["id"] == user_id), None)
    if not user:
        raise ValueError("User not found")

    admins = await list_portal_admins()
    if any(item["username"] == user["username"] for item in admins):
        raise ValueError("Admin already exists for this username")

    await create_portal_admin(username=user["username"], password=password, role=role)


async def authenticate_portal_admin(username: str, password: str) -> Optional[Dict[str, Any]]:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            async with db.execute(
                "SELECT * FROM portal_admins WHERE username = ? AND status = 'active'",
                (username,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                admin = _dict_from_sqlite_row(row)
                if admin.get("password_hash") and check_password_hash(admin["password_hash"], password):
                    admin.pop("password_hash", None)
                    return admin
                return None

    conn = await _pgsql_connect()
    try:
        row = await conn.fetchrow(
            "SELECT * FROM portal_admins WHERE username = $1 AND status = 'active'",
            username,
        )
        if not row:
            return None
        admin = dict(row)
        if admin.get("password_hash") and check_password_hash(admin["password_hash"], password):
            admin.pop("password_hash", None)
            return admin
        return None
    finally:
        await conn.close()


async def update_portal_admin_status(admin_id: str, status: str) -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute("UPDATE portal_admins SET status = ? WHERE id = ?", (status, admin_id))
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute("UPDATE portal_admins SET status = $1 WHERE id = $2", status, admin_id)
    finally:
        await conn.close()


async def delete_portal_admin(admin_id: str) -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute("DELETE FROM portal_admins WHERE id = ?", (admin_id,))
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute("DELETE FROM portal_admins WHERE id = $1", admin_id)
    finally:
        await conn.close()


async def list_points_transactions(limit: int = 50) -> List[Dict[str, Any]]:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            async with db.execute(
                """
                SELECT p.id, p.user_id, u.username, p.amount, p.action, p.description, p.created_at
                FROM points_transactions p
                LEFT JOIN portal_users u ON u.id = p.user_id
                ORDER BY p.created_at DESC
                LIMIT ?
                """,
                (limit,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [_dict_from_sqlite_row(row) for row in rows]

    conn = await _pgsql_connect()
    try:
        rows = await conn.fetch(
            """
            SELECT p.id, p.user_id, u.username, p.amount, p.action, p.description, p.created_at
            FROM points_transactions p
            LEFT JOIN portal_users u ON u.id = p.user_id
            ORDER BY p.created_at DESC
            LIMIT $1
            """,
            limit,
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


async def list_recent_results(limit: int = 20) -> List[Dict[str, Any]]:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            async with db.execute(
                "SELECT task_id, type, data, created_at FROM results ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [_normalize_task_result(_dict_from_sqlite_row(row)) for row in rows]

    conn = await _pgsql_connect()
    try:
        rows = await conn.fetch(
            "SELECT task_id, type, data, created_at FROM results ORDER BY created_at DESC LIMIT $1",
            limit,
        )
        return [_normalize_task_result(dict(row)) for row in rows]
    finally:
        await conn.close()


def generate_service_token() -> str:
    return f"ts_{secrets.token_urlsafe(24)}"


async def list_service_api_keys(owner_id: str, owner_kind: str) -> List[Dict[str, Any]]:
    db_type = _get_db_type()
    query = "SELECT * FROM service_api_keys WHERE owner_id = ? AND owner_kind = ? ORDER BY created_at DESC"
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            async with db.execute(query, (owner_id, owner_kind)) as cursor:
                rows = await cursor.fetchall()
                return [_dict_from_sqlite_row(row) for row in rows]

    conn = await _pgsql_connect()
    try:
        rows = await conn.fetch(
            "SELECT * FROM service_api_keys WHERE owner_id = $1 AND owner_kind = $2 ORDER BY created_at DESC",
            owner_id,
            owner_kind,
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


async def create_service_api_key(owner_id: str, owner_kind: str, name: str, scopes: str, expires_at: str = "") -> None:
    key_id = _generate_id()
    token = generate_service_token()
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute(
                "INSERT INTO service_api_keys (id, owner_id, owner_kind, name, token, scopes, expires_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (key_id, owner_id, owner_kind, name, token, scopes, expires_at or None, "active"),
            )
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute(
            "INSERT INTO service_api_keys (id, owner_id, owner_kind, name, token, scopes, expires_at, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            key_id,
            owner_id,
            owner_kind,
            name,
            token,
            scopes,
            expires_at or None,
            "active",
        )
    finally:
        await conn.close()


async def update_service_api_key_status(key_id: str, status: str) -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute("UPDATE service_api_keys SET status = ? WHERE id = ?", (status, key_id))
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute("UPDATE service_api_keys SET status = $1 WHERE id = $2", status, key_id)
    finally:
        await conn.close()


async def update_service_api_key(key_id: str, name: str, scopes: str, expires_at: str = "") -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute(
                "UPDATE service_api_keys SET name = ?, scopes = ?, expires_at = ? WHERE id = ?",
                (name, scopes, expires_at or None, key_id),
            )
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute(
            "UPDATE service_api_keys SET name = $1, scopes = $2, expires_at = $3 WHERE id = $4",
            name,
            scopes,
            expires_at or None,
            key_id,
        )
    finally:
        await conn.close()


async def delete_service_api_key(key_id: str) -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute("DELETE FROM service_api_keys WHERE id = ?", (key_id,))
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute("DELETE FROM service_api_keys WHERE id = $1", key_id)
    finally:
        await conn.close()


async def list_service_webhooks(owner_id: str, owner_kind: str) -> List[Dict[str, Any]]:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            async with db.execute(
                "SELECT * FROM service_webhooks WHERE owner_id = ? AND owner_kind = ? ORDER BY created_at DESC",
                (owner_id, owner_kind),
            ) as cursor:
                rows = await cursor.fetchall()
                return [_dict_from_sqlite_row(row) for row in rows]

    conn = await _pgsql_connect()
    try:
        rows = await conn.fetch(
            "SELECT * FROM service_webhooks WHERE owner_id = $1 AND owner_kind = $2 ORDER BY created_at DESC",
            owner_id,
            owner_kind,
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


async def create_service_webhook(owner_id: str, owner_kind: str, endpoint: str, events: str, secret: str = "") -> None:
    db_type = _get_db_type()
    webhook_id = _generate_id()
    resolved_secret = secret or secrets.token_hex(8)
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute(
                "INSERT INTO service_webhooks (id, owner_id, owner_kind, endpoint, secret, events, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (webhook_id, owner_id, owner_kind, endpoint, resolved_secret, events, "enabled"),
            )
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute(
            "INSERT INTO service_webhooks (id, owner_id, owner_kind, endpoint, secret, events, status) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            webhook_id,
            owner_id,
            owner_kind,
            endpoint,
            resolved_secret,
            events,
            "enabled",
        )
    finally:
        await conn.close()


async def delete_service_webhook(webhook_id: str) -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute("DELETE FROM service_webhooks WHERE id = ?", (webhook_id,))
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute("DELETE FROM service_webhooks WHERE id = $1", webhook_id)
    finally:
        await conn.close()


async def list_service_ip_whitelist(owner_id: str, owner_kind: str) -> List[Dict[str, Any]]:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            async with db.execute(
                "SELECT * FROM service_ip_whitelist WHERE owner_id = ? AND owner_kind = ? ORDER BY created_at DESC",
                (owner_id, owner_kind),
            ) as cursor:
                rows = await cursor.fetchall()
                return [_dict_from_sqlite_row(row) for row in rows]

    conn = await _pgsql_connect()
    try:
        rows = await conn.fetch(
            "SELECT * FROM service_ip_whitelist WHERE owner_id = $1 AND owner_kind = $2 ORDER BY created_at DESC",
            owner_id,
            owner_kind,
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


async def create_service_ip_whitelist(owner_id: str, owner_kind: str, ip_address: str, note: str = "") -> None:
    db_type = _get_db_type()
    whitelist_id = _generate_id()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute(
                "INSERT INTO service_ip_whitelist (id, owner_id, owner_kind, ip_address, note) VALUES (?, ?, ?, ?, ?)",
                (whitelist_id, owner_id, owner_kind, ip_address, note or None),
            )
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute(
            "INSERT INTO service_ip_whitelist (id, owner_id, owner_kind, ip_address, note) VALUES ($1, $2, $3, $4, $5)",
            whitelist_id,
            owner_id,
            owner_kind,
            ip_address,
            note or None,
        )
    finally:
        await conn.close()


async def delete_service_ip_whitelist(whitelist_id: str) -> None:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute("DELETE FROM service_ip_whitelist WHERE id = ?", (whitelist_id,))
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute("DELETE FROM service_ip_whitelist WHERE id = $1", whitelist_id)
    finally:
        await conn.close()


async def list_service_billing_orders(owner_id: str, owner_kind: str) -> List[Dict[str, Any]]:
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            async with db.execute(
                "SELECT * FROM service_billing_orders WHERE owner_id = ? AND owner_kind = ? ORDER BY created_at DESC",
                (owner_id, owner_kind),
            ) as cursor:
                rows = await cursor.fetchall()
                return [_dict_from_sqlite_row(row) for row in rows]

    conn = await _pgsql_connect()
    try:
        rows = await conn.fetch(
            "SELECT * FROM service_billing_orders WHERE owner_id = $1 AND owner_kind = $2 ORDER BY created_at DESC",
            owner_id,
            owner_kind,
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


async def create_service_billing_order(owner_id: str, owner_kind: str, amount: float, points: int, description: str = "") -> None:
    order_id = _generate_id()
    db_type = _get_db_type()
    if db_type == "sqlite":
        async with await _sqlite_connect() as db:
            await db.execute(
                "INSERT INTO service_billing_orders (id, owner_id, owner_kind, amount, points, status, description) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (order_id, owner_id, owner_kind, amount, points, "paid", description or "账户充值"),
            )
            if owner_kind == "user":
                await db.execute("UPDATE portal_users SET points = points + ? WHERE id = ?", (points, owner_id))
                await db.execute(
                    "INSERT INTO points_transactions (id, user_id, amount, action, description) VALUES (?, ?, ?, ?, ?)",
                    (_generate_id(), owner_id, points, "recharge", description or "账户充值"),
                )
            await db.commit()
            return

    conn = await _pgsql_connect()
    try:
        await conn.execute(
            "INSERT INTO service_billing_orders (id, owner_id, owner_kind, amount, points, status, description) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            order_id,
            owner_id,
            owner_kind,
            amount,
            points,
            "paid",
            description or "账户充值",
        )
        if owner_kind == "user":
            await conn.execute("UPDATE portal_users SET points = points + $1 WHERE id = $2", points, owner_id)
            await conn.execute(
                "INSERT INTO points_transactions (id, user_id, amount, action, description) VALUES ($1, $2, $3, $4, $5)",
                _generate_id(),
                owner_id,
                points,
                "recharge",
                description or "账户充值",
            )
    finally:
        await conn.close()


async def get_portal_stats() -> Dict[str, Any]:
    recent_results = await list_recent_results(limit=200)
    users = await list_portal_users()
    admins = await list_portal_admins()
    transactions = await list_points_transactions(limit=100)

    tasks_total = len(recent_results)
    tasks_ready = sum(1 for item in recent_results if item["status"] == "ready")
    tasks_failed = sum(1 for item in recent_results if item["status"] == "failed")
    tasks_pending = sum(1 for item in recent_results if item["status"] == "processing")
    elapsed_values = [
        parsed
        for parsed in (_parse_elapsed_seconds(item.get("elapsed_time")) for item in recent_results)
        if parsed is not None
    ]
    elapsed_max = max(elapsed_values) if elapsed_values else None
    elapsed_min = min(elapsed_values) if elapsed_values else None
    elapsed_avg = (sum(elapsed_values) / len(elapsed_values)) if elapsed_values else None

    return {
        "users_total": len(users),
        "users_active": sum(1 for item in users if item.get("status") == "active"),
        "admins_total": len(admins),
        "admins_active": sum(1 for item in admins if item.get("status") == "active"),
        "points_issued": sum(int(item.get("amount") or 0) for item in transactions if int(item.get("amount") or 0) > 0),
        "points_consumed": abs(sum(int(item.get("amount") or 0) for item in transactions if int(item.get("amount") or 0) < 0)),
        "tasks_total": tasks_total,
        "tasks_ready": tasks_ready,
        "tasks_failed": tasks_failed,
        "tasks_pending": tasks_pending,
        "task_elapsed_max_seconds": elapsed_max,
        "task_elapsed_min_seconds": elapsed_min,
        "task_elapsed_avg_seconds": elapsed_avg,
        "task_elapsed_max": _format_elapsed_seconds(elapsed_max),
        "task_elapsed_min": _format_elapsed_seconds(elapsed_min),
        "task_elapsed_avg": _format_elapsed_seconds(elapsed_avg),
    }
