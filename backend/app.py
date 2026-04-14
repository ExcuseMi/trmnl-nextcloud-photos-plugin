import logging
import os

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
    nextcloud_url = request.args.get('nextcloud_url', '')
    username = request.args.get('username', '')
    token = request.args.get('token', '')
    folder = request.args.get('folder', '/Photos')
    mode = request.args.get('mode', 'sequential')
    recursive = request.args.get('recursive', 'true').lower() != 'false'

    if not (nextcloud_url and username and token):
        return jsonify({'image_url': None, 'image_path': '', 'error': 'Missing nextcloud_url, username, or token'}), 400

    try:
        images = await list_images(nextcloud_url, username, token, folder, recursive=recursive)
        key = instance_key(nextcloud_url, username, folder)
        selected = await pick_image(images, mode, key)

        if not selected:
            return jsonify({'image_url': None, 'image_path': '', 'error': 'No images found'})

        image_url = await get_direct_link(nextcloud_url, username, token, selected['file_id'])
        return jsonify({'image_url': image_url, 'image_path': selected['path']})

    except Exception as exc:
        log.exception('Error fetching image')
        return jsonify({'image_url': None, 'image_path': '', 'error': str(exc)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
