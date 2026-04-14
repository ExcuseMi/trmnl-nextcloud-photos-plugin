import logging
import os
from urllib.parse import quote, unquote

import aiohttp
from quart import Quart, Response, jsonify, request

from modules.providers.nextcloud import list_images
from modules.utils.ip_whitelist import init_ip_whitelist, require_trmnl_ip
from modules.utils.state import instance_key, pick_image

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
log = logging.getLogger(__name__)

app = Quart(__name__)

BACKEND_URL = os.getenv('BACKEND_URL', 'http://localhost:8080')


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

        image_url = (
            f"{BACKEND_URL}/image/preview"
            f"?nextcloud_url={quote(nextcloud_url)}"
            f"&username={quote(username)}"
            f"&token={quote(token)}"
            f"&file_id={quote(selected['file_id'])}"
        )
        return jsonify({'image_url': image_url, 'image_path': selected['path']})

    except Exception as exc:
        log.exception('Error fetching image')
        return jsonify({'image_url': None, 'image_path': '', 'error': str(exc)}), 500


@app.route('/image/preview')
@require_trmnl_ip
async def image_preview():
    """Proxy Nextcloud's server-side resized preview — no PIL needed."""
    nextcloud_url = request.args.get('nextcloud_url', '')
    username = request.args.get('username', '')
    token = request.args.get('token', '')
    file_id = request.args.get('file_id', '')
    width = request.args.get('w', '800')
    height = request.args.get('h', '480')

    if not (nextcloud_url and username and token and file_id):
        return 'missing required params', 400

    url = (
        f"{nextcloud_url.rstrip('/')}/index.php/core/preview"
        f"?fileId={file_id}&x={width}&y={height}&a=1&forceIcon=0"
    )
    auth = aiohttp.BasicAuth(username, token)

    session = aiohttp.ClientSession()
    resp = await session.get(url, auth=auth, timeout=aiohttp.ClientTimeout(total=30))
    resp.raise_for_status()
    content_type = resp.headers.get('Content-Type', 'image/jpeg')

    async def generate():
        try:
            async for chunk in resp.content.iter_chunked(8192):
                yield chunk
        finally:
            resp.close()
            await session.close()

    return Response(generate(), content_type=content_type)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
