import asyncio
import io
import logging
import os
from urllib.parse import quote, unquote

import aiohttp
from PIL import Image, ImageOps
from quart import Quart, Response, jsonify, request

from modules.providers.nextcloud import fetch_original, fetch_photo_metadata, list_images
from modules.utils.geocode import reverse_geocode
from modules.utils.ip_whitelist import init_ip_whitelist, require_trmnl_ip
from modules.utils.state import init_db, instance_key, pick_image

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
log = logging.getLogger(__name__)

app = Quart(__name__)

BACKEND_URL = os.getenv('BACKEND_URL', 'http://localhost:8080')


@app.before_serving
async def _startup():
    await asyncio.gather(
        init_db(),
        init_ip_whitelist()
    )


@app.route('/health')
async def health():
    return jsonify({'ok': True})


@app.route('/image', methods=['POST'])
@require_trmnl_ip
async def image():
    body = await request.get_json(silent=True, force=True) or {}
    nextcloud_url = body.get('nextcloud_url', '').rstrip('/')
    username = body.get('username', '')
    token = body.get('token', '')
    folder = body.get('folder', '/Photos')
    mode = body.get('mode', 'sequential')
    recursive = str(body.get('recursive', 'true')).lower() != 'false'
    plugin_setting_id = str(body.get('plugin_setting_id', ''))

    device = body.get('device') or {}
    width = int(device.get('width', 800))
    height = int(device.get('height', 480))

    if not (nextcloud_url and username and token):
        return _error('Missing nextcloud_url, username, or token')

    try:
        images = await list_images(nextcloud_url, username, token, folder, recursive=recursive)
    except aiohttp.ClientResponseError as e:
        if e.status == 401:
            return _error('Nextcloud authentication failed — check username and app token')
        if e.status == 404:
            return _error(f'Folder not found: {folder}')
        if e.status in (502, 503):
            return _error('Nextcloud unavailable')
        log.error('Nextcloud PROPFIND error %s: %s', e.status, e.message)
        return _error(f'Nextcloud error {e.status}')
    except aiohttp.ClientConnectorError:
        return _error(f'Could not connect to Nextcloud at {nextcloud_url}')
    except aiohttp.ServerTimeoutError:
        return _error('Nextcloud connection timed out')
    except Exception as e:
        log.exception('Error listing images')
        return _error(str(e))

    if not images:
        return _error(f'No images found in {folder}')

    key = plugin_setting_id or instance_key(nextcloud_url, username, folder)

    try:
        selected = await pick_image(images, mode, key)
    except Exception as e:
        log.exception('Error picking image')
        return _error(str(e))

    if not selected:
        return _error(f'No images found in {folder}')

    image_url = (
        f"{BACKEND_URL}/image/preview"
        f"?file_id={quote(selected['file_id'])}"
        f"&nextcloud_url={quote(nextcloud_url)}"
        f"&username={quote(username)}"
        f"&token={quote(token)}"
        f"&path={quote(selected['path'])}"
        f"&w={width}&h={height}"
    )

    metadata = {}
    try:
        metadata = await fetch_photo_metadata(
            nextcloud_url, username, token, selected['path'], selected['file_id']
        )
    except Exception:
        log.exception('Error fetching photo metadata for %s', selected['path'])

    if metadata.get('gps_lat') is not None and metadata.get('gps_lon') is not None:
        try:
            metadata['location'] = await reverse_geocode(metadata['gps_lat'], metadata['gps_lon'])
        except Exception:
            log.warning('Geocoding failed for %s', selected['path'])

    seq_position = None
    if mode == 'sequential':
        seq_position = next(
            (i + 1 for i, img in enumerate(images) if img['href'] == selected['href']),
            None,
        )

    log.info('Serving %s at %dx%d (%s)', selected['path'], width, height, mode)
    return jsonify({
        'image_url': image_url,
        'image_path': selected['path'],
        'folder_count': len(images),
        'seq_position': seq_position,
        'metadata': metadata,
        'error': None,
    })


@app.route('/image/preview')
@require_trmnl_ip
async def image_preview():
    """Proxy Nextcloud's server-side preview, resized to device dimensions."""
    file_id = request.args.get('file_id', '')
    nextcloud_url = request.args.get('nextcloud_url', '')
    username = request.args.get('username', '')
    token = request.args.get('token', '')
    path = unquote(request.args.get('path', ''))
    width = request.args.get('w', '800')
    height = request.args.get('h', '480')

    if not (file_id and nextcloud_url and username and token):
        return 'missing required params', 400

    url = (
        f"{nextcloud_url.rstrip('/')}/index.php/core/preview"
        f"?fileId={file_id}&x={width}&y={height}&a=1&forceIcon=0"
    )
    auth = aiohttp.BasicAuth(username, token)

    session = aiohttp.ClientSession()
    try:
        resp = await session.get(url, auth=auth, timeout=aiohttp.ClientTimeout(total=30))
        resp.raise_for_status()
    except Exception as e:
        await session.close()
        log.warning('Nextcloud preview generation failed for %s: %s', path or file_id, e)
        if path:
            try:
                data = await fetch_original(nextcloud_url, username, token, path)
                resized = _resize_jpeg(data, int(width), int(height))
                return Response(resized, content_type='image/jpeg')
            except Exception:
                log.exception('Fallback to original image also failed for %s', path)
        return 'preview unavailable', 502

    content_type = resp.headers.get('Content-Type', 'image/jpeg')

    async def generate():
        try:
            async for chunk in resp.content.iter_chunked(8192):
                yield chunk
        finally:
            resp.close()
            await session.close()

    return Response(generate(), content_type=content_type)


def _resize_jpeg(data: bytes, width: int, height: int) -> bytes:
    """Downscale raw image bytes to fit within width x height, re-encoded as JPEG."""
    img = Image.open(io.BytesIO(data))
    img = ImageOps.exif_transpose(img)
    img.thumbnail((width, height), Image.LANCZOS)
    if img.mode not in ('RGB', 'L'):
        img = img.convert('RGB')
    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=85)
    return buf.getvalue()


def _error(message: str):
    log.warning('Returning error: %s', message)
    return jsonify({'image_url': None, 'image_path': '', 'error': message})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
