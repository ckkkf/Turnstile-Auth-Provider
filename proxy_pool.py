import json
from pathlib import Path
from typing import Any, Dict, List, Optional


PROXY_POOL_PATH = Path("proxy_pools.json")


DEFAULT_STATE: Dict[str, Any] = {
    "active_pool_id": None,
    "pools": [],
}


def _read_state() -> Dict[str, Any]:
    if not PROXY_POOL_PATH.exists():
        return dict(DEFAULT_STATE)
    try:
        return json.loads(PROXY_POOL_PATH.read_text(encoding="utf-8"))
    except Exception:
        return dict(DEFAULT_STATE)


def _write_state(state: Dict[str, Any]) -> None:
    PROXY_POOL_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def list_proxy_pools() -> List[Dict[str, Any]]:
    state = _read_state()
    active_pool_id = state.get("active_pool_id")
    pools = []
    for pool in state.get("pools", []):
        item = dict(pool)
        item["is_active"] = pool.get("id") == active_pool_id
        item["size"] = len(pool.get("proxies", []))
        pools.append(item)
    return pools


def create_proxy_pool(name: str, strategy: str = "round_robin", enabled: bool = True) -> Dict[str, Any]:
    state = _read_state()
    pool_id = name.lower().replace(" ", "-")
    suffix = 1
    existing_ids = {item["id"] for item in state.get("pools", [])}
    base_id = pool_id or "proxy-pool"
    pool_id = base_id
    while pool_id in existing_ids:
        suffix += 1
        pool_id = f"{base_id}-{suffix}"

    pool = {
        "id": pool_id,
        "name": name,
        "strategy": strategy,
        "enabled": enabled,
        "proxies": [],
        "cursor": 0,
    }
    state.setdefault("pools", []).append(pool)
    if not state.get("active_pool_id"):
        state["active_pool_id"] = pool_id
    _write_state(state)
    return pool


def update_proxy_pool(pool_id: str, name: str, strategy: str, enabled: bool) -> None:
    state = _read_state()
    for pool in state.get("pools", []):
        if pool["id"] == pool_id:
            pool["name"] = name
            pool["strategy"] = strategy
            pool["enabled"] = enabled
            break
    _write_state(state)


def delete_proxy_pool(pool_id: str) -> None:
    state = _read_state()
    state["pools"] = [item for item in state.get("pools", []) if item["id"] != pool_id]
    if state.get("active_pool_id") == pool_id:
        state["active_pool_id"] = state["pools"][0]["id"] if state.get("pools") else None
    _write_state(state)


def set_active_proxy_pool(pool_id: str) -> None:
    state = _read_state()
    if any(item["id"] == pool_id for item in state.get("pools", [])):
        state["active_pool_id"] = pool_id
        _write_state(state)


def import_proxies(pool_id: str, raw_text: str) -> int:
    lines = [line.strip() for line in raw_text.replace("\r", "").split("\n") if line.strip()]
    state = _read_state()
    count = 0
    for pool in state.get("pools", []):
        if pool["id"] == pool_id:
            existing = set(pool.get("proxies", []))
            for line in lines:
                if line not in existing:
                    pool.setdefault("proxies", []).append(line)
                    existing.add(line)
                    count += 1
            break
    _write_state(state)
    return count


def remove_proxy(pool_id: str, proxy_value: str) -> None:
    state = _read_state()
    for pool in state.get("pools", []):
        if pool["id"] == pool_id:
            pool["proxies"] = [item for item in pool.get("proxies", []) if item != proxy_value]
            break
    _write_state(state)


def get_active_proxy_pool() -> Optional[Dict[str, Any]]:
    state = _read_state()
    active_id = state.get("active_pool_id")
    for pool in state.get("pools", []):
        if pool["id"] == active_id:
            item = dict(pool)
            item["is_active"] = True
            return item
    return None


def select_proxy() -> Optional[str]:
    state = _read_state()
    active_id = state.get("active_pool_id")
    for pool in state.get("pools", []):
        if pool["id"] == active_id and pool.get("enabled"):
            proxies = pool.get("proxies", [])
            if not proxies:
                return None
            strategy = pool.get("strategy", "round_robin")
            if strategy == "random":
                import random
                return random.choice(proxies)
            if strategy == "first":
                return proxies[0]
            cursor = int(pool.get("cursor", 0))
            value = proxies[cursor % len(proxies)]
            pool["cursor"] = (cursor + 1) % len(proxies)
            _write_state(state)
            return value
    return None


def ensure_default_proxy_pool() -> None:
    state = _read_state()
    if not state.get("pools"):
        state["pools"] = [
            {
                "id": "default-pool",
                "name": "默认代理池",
                "strategy": "round_robin",
                "enabled": True,
                "proxies": [],
                "cursor": 0,
            }
        ]
        state["active_pool_id"] = "default-pool"
        _write_state(state)
