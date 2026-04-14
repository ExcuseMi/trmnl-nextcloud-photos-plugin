import logging
import os

import aiohttp
from quart import Quart, jsonify, request

from modules.providers.nextcloud import get_direct_link, list_images
from modules.utils.ip_whitelist import init_ip_whitelist, require_trmnl_ip
from modules.utils.state import instance_key, pick_image

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
log = logging.getLogger(__name__)

app = Quart(__name__)


@app.before_serving
async def _startup():
    await init_ip_whitelist()


@app.route('/health')
async def health():
    return jsonify({'ok': True})


@app.route('/image')
@require_trmnl_ip
async def image():
    nextcloud_url = request.args.get('nextcloud_url', '').rstrip('/')
    username = request.args.get('username', '')
    token = request.args.get('token', '')
    folder = request.args.get('folder', '/Photos')
    mode = request.args.get('mode', 'sequential')
    recursive = request.args.get('recursive', 'true').lower() != 'false'

    if not (nextcloud_url and username and token):
        return _error('Missing nextcloud_url, username, or token'), 400

    try:
        images = await list_images(nextcloud_url, username, token, folder, recursive=recursive)
    except aiohttp.ClientResponseError as e:
        if e.status == 401:
            return _error('Nextcloud authentication failed — check username and app token'), 401
        if e.status == 404:
            return _error(f'Folder not found: {folder}'), 404
        if e.status in (502, 503):
            return _error('Nextcloud unavailable'), 503
        log.error('Nextcloud PROPFIND error %s: %s', e.status, e.message)
        return _error(f'Nextcloud error {e.status}'), 502
    except aiohttp.ClientConnectorError:
        return _error(f'Could not connect to Nextcloud at {nextcloud_url}'), 502
    except aiohttp.ServerTimeoutError:
        return _error('Nextcloud connection timed out'), 504
    except Exception as e:
        log.exception('Error listing images')
        return _error(str(e)), 500

    if not images:
        return _error(f'No images found in {folder}')

    key = instance_key(nextcloud_url, username, folder)

    try:
        selected = await pick_image(images, mode, key)
    except Exception as e:
        log.exception('Error picking image')
        return _error(str(e)), 500

    if not selected:
        return _error(f'No images found in {folder}')

    try:
        image_url = await get_direct_link(nextcloud_url, username, token, selected['file_id'])
    except aiohttp.ClientResponseError as e:
        if e.status == 404:
            return _error('OCS Direct Link API not available — requires Nextcloud 18+'), 502
        if e.status == 401:
            return _error('Nextcloud authentication failed — check username and app token'), 401
        log.error('OCS direct link error %s', e.status)
        return _error(f'Failed to get image URL (Nextcloud error {e.status})'), 502
    except Exception as e:
        log.exception('Error getting direct link')
        return _error(str(e)), 500

    log.info('Serving %s (%s)', selected['path'], mode)
    return jsonify({'image_url': image_url, 'image_path': selected['path'], 'error': None})


def _error(message: str) -> 'quart.Response':
    log.warning('Returning error: %s', message)
    return jsonify({'image_url': None, 'image_path': '', 'error': message})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
