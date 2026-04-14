import logging
import os
import time

import aiohttp
import aiosqlite

log = logging.getLogger(__name__)

DB_PATH = os.getenv('STATE_DB', '/data/state.db')
GEOCODE_TTL = 30 * 24 * 3600  # 30 days — towns don't move


async def _init_table(db: aiosqlite.Connection):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS geocode_cache (
            key      TEXT PRIMARY KEY,
            location TEXT,
            cached_at INTEGER NOT NULL
        )
    """)
    await db.commit()


def _key(lat: float, lon: float) -> str:
    """Round to 2 decimal places (~1.1 km grid) so nearby photos share a cache entry."""
    return f"{lat:.2f},{lon:.2f}"


async def reverse_geocode(lat: float, lon: float) -> str | None:
    key = _key(lat, lon)

    # --- cache read ---
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await _init_table(db)
            async with db.execute(
                "SELECT location, cached_at FROM geocode_cache WHERE key = ?", (key,)
            ) as cur:
                row = await cur.fetchone()
            if row:
                location, cached_at = row
                if time.time() - cached_at < GEOCODE_TTL:
                    return location or None  # None means "no result" — don't retry until TTL
    except Exception as exc:
        log.warning('Geocode cache read failed: %s', exc)

    # --- Nominatim lookup ---
    location = None
    try:
        url = (
            f"https://nominatim.openstreetmap.org/reverse"
            f"?lat={lat}&lon={lon}&format=json&zoom=10"
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={'User-Agent': 'TRMNL-Nextcloud-Plugin/1.0 (self-hosted)'},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    addr = data.get('address', {})
                    location = (
                        addr.get('city')
                        or addr.get('town')
                        or addr.get('village')
                        or addr.get('municipality')
                        or addr.get('county')
                    )
    except Exception as exc:
        log.warning('Nominatim lookup failed for %.4f,%.4f: %s', lat, lon, exc)

    # --- cache write (store None too, to suppress future retries until TTL) ---
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await _init_table(db)
            await db.execute(
                "INSERT OR REPLACE INTO geocode_cache (key, location, cached_at) VALUES (?, ?, ?)",
                (key, location, int(time.time())),
            )
            await db.commit()
    except Exception as exc:
        log.warning('Geocode cache write failed: %s', exc)

    return location
