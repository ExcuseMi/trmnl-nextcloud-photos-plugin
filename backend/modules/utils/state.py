import asyncio
import hashlib
import json
import logging
import os
import random

import asyncpg

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/postgres')

_pool = None


async def get_pool():
    global _pool
    if _pool is None:
        await init_db()
    return _pool


async def init_db():
    global _pool
    if _pool is not None:
        return _pool

    # Retry logic for startup (Postgres might be booting)
    for i in range(10):
        try:
            _pool = await asyncpg.create_pool(DATABASE_URL)
            async with _pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS instance_state (
                        key TEXT PRIMARY KEY,
                        state JSONB NOT NULL
                    )
                """)
            log.info("PostgreSQL connection pool initialized")
            break
        except Exception as e:
            if i == 9:
                log.error("Failed to connect to PostgreSQL after 10 attempts: %s", e)
                raise
            log.warning("PostgreSQL not ready, retrying in 2s... (%d/10)", i + 1)
            await asyncio.sleep(2)
    return _pool


def instance_key(nextcloud_url: str, username: str, folder_path: str) -> str:
    raw = f"{nextcloud_url}|{username}|{folder_path}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


async def load_state(key: str) -> dict:
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT state FROM instance_state WHERE key = $1", key)
            if row:
                return json.loads(row['state']) if isinstance(row['state'], str) else row['state']
    except Exception as exc:
        log.warning('Could not load state for %s: %s', key, exc)
    return {'current_index': 0, 'shuffle_order': [], 'last_path': None}


async def save_state(key: str, state: dict):
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO instance_state (key, state) VALUES ($1, $2) "
                "ON CONFLICT (key) DO UPDATE SET state = EXCLUDED.state",
                key, json.dumps(state),
            )
    except Exception as exc:
        log.warning('Could not save state for %s: %s', key, exc)


async def pick_image(images: list[dict], mode: str, key: str) -> dict | None:
    if not images:
        return None

    if mode == 'newest':
        return max(images, key=lambda x: x['last_modified'])

    if mode == 'oldest':
        return min(images, key=lambda x: x['last_modified'])

    if mode == 'random':
        return random.choice(images)

    state = await load_state(key)
    hrefs = [img['href'] for img in images]
    img_map = {img['href']: img for img in images}

    if mode == 'sequential':
        idx = state.get('current_index', 0)
        if idx >= len(images):
            idx = 0
        selected = images[idx]
        state['current_index'] = (idx + 1) % len(images)
        state['last_path'] = selected['path']
        await save_state(key, state)
        return selected

    if mode == 'shuffle':
        order = state.get('shuffle_order', [])
        valid_order = [h for h in order if h in img_map]
        if not valid_order:
            valid_order = hrefs.copy()
            random.shuffle(valid_order)
            state['current_index'] = 0

        idx = state.get('current_index', 0)
        if idx >= len(valid_order):
            random.shuffle(valid_order)
            idx = 0

        selected = img_map[valid_order[idx]]
        state['shuffle_order'] = valid_order
        state['current_index'] = idx + 1
        state['last_path'] = selected['path']
        await save_state(key, state)
        return selected

    selected = random.choice(images)
    state['last_path'] = selected['path']
    await save_state(key, state)
    return selected
