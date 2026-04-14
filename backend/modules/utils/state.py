import hashlib
import json
import logging
import os
import random

import aiosqlite

log = logging.getLogger(__name__)

DB_PATH = os.getenv('STATE_DB', '/data/state.db')


async def _init_db(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS instance_state (
            key TEXT PRIMARY KEY,
            state TEXT NOT NULL
        )
    """)
    await db.commit()


def instance_key(nextcloud_url: str, username: str, folder_path: str) -> str:
    raw = f"{nextcloud_url}|{username}|{folder_path}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


async def load_state(key: str) -> dict:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await _init_db(db)
            async with db.execute("SELECT state FROM instance_state WHERE key = ?", (key,)) as cur:
                row = await cur.fetchone()
                if row:
                    return json.loads(row[0])
    except Exception as exc:
        log.warning('Could not load state for %s: %s', key, exc)
    return {'current_index': 0, 'shuffle_order': [], 'last_path': None}


async def save_state(key: str, state: dict):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await _init_db(db)
            await db.execute(
                "INSERT OR REPLACE INTO instance_state (key, state) VALUES (?, ?)",
                (key, json.dumps(state)),
            )
            await db.commit()
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
