import logging
from xml.etree import ElementTree

import aiohttp

log = logging.getLogger(__name__)

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.tiff'}

PROPFIND_BODY = """<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:prop>
    <d:getlastmodified/>
    <d:getcontenttype/>
    <d:resourcetype/>
    <oc:fileid/>
  </d:prop>
</d:propfind>"""


def _dav_url(nextcloud_url: str, username: str, folder_path: str) -> str:
    base = nextcloud_url.rstrip('/')
    folder = folder_path.lstrip('/')
    return f"{base}/remote.php/dav/files/{username}/{folder}"


async def list_images(
    nextcloud_url: str,
    username: str,
    app_token: str,
    folder_path: str,
    recursive: bool = True,
) -> list[dict]:
    """Return all image entries under folder_path, optionally recursive."""
    auth = aiohttp.BasicAuth(username, app_token)
    depth = 'infinity' if recursive else '1'

    async with aiohttp.ClientSession() as session:
        async with session.request(
            'PROPFIND',
            _dav_url(nextcloud_url, username, folder_path),
            data=PROPFIND_BODY,
            headers={'Depth': depth, 'Content-Type': 'application/xml'},
            auth=auth,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            content = await resp.read()

    ns = {'d': 'DAV:', 'oc': 'http://owncloud.org/ns'}
    tree = ElementTree.fromstring(content)
    images = []

    for response in tree.findall('d:response', ns):
        href = response.findtext('d:href', namespaces=ns) or ''
        content_type = response.findtext('.//d:getcontenttype', namespaces=ns) or ''
        resource_type = response.find('.//d:resourcetype/d:collection', ns)

        if resource_type is not None:
            continue

        ext = '.' + href.rsplit('.', 1)[-1].lower() if '.' in href else ''
        if ext not in IMAGE_EXTENSIONS and not content_type.startswith('image/'):
            continue

        file_id = response.findtext('.//oc:fileid', namespaces=ns) or ''
        last_modified = response.findtext('.//d:getlastmodified', namespaces=ns) or ''
        # Extract path relative to /remote.php/dav/files/{username}/
        dav_prefix = '/remote.php/dav/files/'
        rel_path = href
        if dav_prefix in href:
            after_prefix = href.split(dav_prefix, 1)[1]
            rel_path = '/' + after_prefix.split('/', 1)[1] if '/' in after_prefix else href
        images.append({
            'href': href,
            'file_id': file_id,
            'last_modified': last_modified,
            'name': href.rsplit('/', 1)[-1],
            'path': rel_path,
        })

    return sorted(images, key=lambda x: x['href'])


async def get_direct_link(nextcloud_url: str, username: str, app_token: str, file_id: str) -> str:
    """Get an 8-hour public direct download URL via OCS API (no auth required to use it)."""
    url = f"{nextcloud_url.rstrip('/')}/ocs/v2.php/apps/dav/api/v1/direct"
    auth = aiohttp.BasicAuth(username, app_token)

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url,
            data={'fileId': file_id},
            headers={'OCS-APIRequest': 'true', 'Accept': 'application/json'},
            auth=auth,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

    return data['ocs']['data']['url']

