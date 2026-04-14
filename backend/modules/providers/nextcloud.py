import json
import logging
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree

import aiohttp

log = logging.getLogger(__name__)

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.tiff'}

PROPFIND_BODY = """<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">
  <d:prop>
    <d:getlastmodified/>
    <d:getcontenttype/>
    <d:resourcetype/>
    <oc:fileid/>
    <nc:creation_time/>
  </d:prop>
</d:propfind>"""

METADATA_PROPFIND_BODY = """<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">
  <d:prop>
    <d:getcontentlength/>
    <nc:creation_time/>
    <nc:upload_time/>
    <nc:metadata_photos_size/>
    <nc:metadata_photos_gps/>
    <nc:metadata-photos-ifd0/>
    <nc:metadata-photos-exif/>
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

    ns = {'d': 'DAV:', 'oc': 'http://owncloud.org/ns', 'nc': 'http://nextcloud.org/ns'}
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
        creation_time = response.findtext('.//nc:creation_time', namespaces=ns) or ''

        # Prefer nc:creation_time (EXIF capture date) for sorting; fall back to getlastmodified.
        date_ts: int | None = None
        if creation_time:
            try:
                date_ts = int(creation_time)
            except ValueError:
                pass
        if date_ts is None and last_modified:
            try:
                date_ts = int(parsedate_to_datetime(last_modified).timestamp())
            except Exception:
                pass

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
            'date_ts': date_ts,
            'name': href.rsplit('/', 1)[-1],
            'path': rel_path,
        })

    return sorted(images, key=lambda x: x['href'])


async def fetch_photo_metadata(
    nextcloud_url: str,
    username: str,
    app_token: str,
    file_path: str,
) -> dict:
    """Fetch rich EXIF/photo metadata for a single file via PROPFIND Depth:0.

    Returns a dict with whatever Nextcloud's metadata extractor has indexed.
    Fields depend on Nextcloud version and whether the Photos app is active.
    """
    url = _dav_url(nextcloud_url, username, file_path)
    auth = aiohttp.BasicAuth(username, app_token)
    ns = {'d': 'DAV:', 'oc': 'http://owncloud.org/ns', 'nc': 'http://nextcloud.org/ns'}

    async with aiohttp.ClientSession() as session:
        async with session.request(
            'PROPFIND', url,
            data=METADATA_PROPFIND_BODY,
            headers={'Depth': '0', 'Content-Type': 'application/xml'},
            auth=auth,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status not in (200, 207):
                log.warning('Metadata PROPFIND returned %s for %s', resp.status, file_path)
                return {}
            content = await resp.read()

    tree = ElementTree.fromstring(content)
    response = tree.find('d:response', ns)
    if not response:
        return {}

    meta: dict = {}

    # File size (bytes)
    raw_size = response.findtext('.//d:getcontentlength', namespaces=ns)
    if raw_size:
        try:
            meta['file_size'] = int(raw_size)
        except ValueError:
            pass

    # Capture date (Unix timestamp — EXIF DateTimeOriginal via nc:creation_time)
    raw_creation = response.findtext('.//nc:creation_time', namespaces=ns)
    if raw_creation:
        try:
            meta['date_ts'] = int(raw_creation)
        except ValueError:
            pass

    # Upload date (Unix timestamp)
    raw_upload = response.findtext('.//nc:upload_time', namespaces=ns)
    if raw_upload:
        try:
            meta['upload_ts'] = int(raw_upload)
        except ValueError:
            pass

    # Photo dimensions — JSON: {"width": 4032, "height": 3024}
    raw_size_json = response.findtext('.//nc:metadata_photos_size', namespaces=ns)
    if raw_size_json:
        try:
            size_data = json.loads(raw_size_json)
            meta['width'] = size_data.get('width')
            meta['height'] = size_data.get('height')
        except (json.JSONDecodeError, AttributeError):
            pass

    # GPS — JSON: {"latitude": 50.85, "longitude": 4.35, "altitude": 12.0}
    raw_gps = response.findtext('.//nc:metadata_photos_gps', namespaces=ns)
    if raw_gps:
        try:
            gps = json.loads(raw_gps)
            meta['gps_lat'] = gps.get('latitude')
            meta['gps_lon'] = gps.get('longitude')
            meta['gps_alt'] = gps.get('altitude')
        except (json.JSONDecodeError, AttributeError):
            pass

    # Camera make/model — JSON: {"Make": "Apple", "Model": "iPhone 15 Pro"}
    raw_ifd0 = response.findtext('.//nc:metadata-photos-ifd0', namespaces=ns)
    if raw_ifd0:
        try:
            ifd0 = json.loads(raw_ifd0)
            meta['camera_make'] = ifd0.get('Make')
            meta['camera_model'] = ifd0.get('Model')
        except (json.JSONDecodeError, AttributeError):
            pass

    # Exposure data — JSON: {"FNumber": 1.78, "ExposureTime": 0.008, "ISOSpeedRatings": 400, "FocalLength": 24.0}
    raw_exif = response.findtext('.//nc:metadata-photos-exif', namespaces=ns)
    if raw_exif:
        try:
            exif = json.loads(raw_exif)
            meta['aperture'] = exif.get('FNumber')
            meta['shutter_speed'] = exif.get('ExposureTime')
            meta['iso'] = exif.get('ISOSpeedRatings')
            meta['focal_length'] = exif.get('FocalLength')
        except (json.JSONDecodeError, AttributeError):
            pass

    return meta
