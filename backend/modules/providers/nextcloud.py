import hashlib
import logging
from datetime import datetime
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree

import io

import aiohttp
import piexif
from PIL import Image

from modules.utils.redis_cache import get_cached_json, set_cached_json

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

EXIF_RANGE = 'bytes=0-65535'  # first 64 KB — enough for all EXIF headers


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
    raw_key = f"list:{nextcloud_url}:{username}:{folder_path}:{recursive}"
    cache_key = hashlib.sha256(raw_key.encode()).hexdigest()
    cached = await get_cached_json(cache_key)
    if cached:
        return cached

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

    result = sorted(images, key=lambda x: x['href'])
    await set_cached_json(cache_key, result, ttl=300)  # Cache list for 5 minutes
    return result


NC_METADATA_BODY = """<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">
  <d:prop>
    <d:getlastmodified/>
    <d:getcontenttype/>
    <oc:size/>
    <nc:creation_time/>
    <nc:width/>
    <nc:height/>
    <nc:latitude/>
    <nc:longitude/>
    <nc:camera_make/>
    <nc:camera_model/>
    <nc:f_number/>
    <nc:exposure_time/>
    <nc:iso/>
    <nc:focal_length/>
  </d:prop>
</d:propfind>"""


async def fetch_photo_metadata(
    nextcloud_url: str,
    username: str,
    app_token: str,
    file_path: str,
    file_id: str = None,
) -> dict:
    """Fetch metadata, prioritizing Nextcloud's own indexed properties via PROPFIND.

    Falls back to a 64 KB range request for EXIF if Nextcloud hasn't indexed the file.
    Uses file_id to fetch a small preview for brightness score if EXIF thumbnail is missing.
    """
    raw_key = f"meta:{nextcloud_url}:{file_path}:{file_id}"
    cache_key = hashlib.sha256(raw_key.encode()).hexdigest()
    cached = await get_cached_json(cache_key)
    if cached:
        return cached

    url = _dav_url(nextcloud_url, username, file_path)
    auth = aiohttp.BasicAuth(username, app_token)
    meta: dict = {}

    # 1. Try Nextcloud PROPFIND first
    try:
        async with aiohttp.ClientSession() as session:
            async with session.request(
                'PROPFIND',
                url,
                data=NC_METADATA_BODY,
                headers={'Depth': '0', 'Content-Type': 'application/xml'},
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (200, 207):
                    content = await resp.read()
                    _extract_nc_meta(content, meta)
    except Exception:
        log.exception('Nextcloud PROPFIND failed for %s', file_path)

    # 2. If critical metadata is missing, try EXIF range request
    # We consider it "missing" if we don't even have basic exposure or camera info
    needs_exif = not any([meta.get('camera_model'), meta.get('shutter_speed'), meta.get('width')])
    
    ext = file_path.rsplit('.', 1)[-1].lower() if '.' in file_path else ''
    is_jpeg = ext in ('jpg', 'jpeg', 'tif', 'tiff')
    thumbnail_bytes = None

    if needs_exif and is_jpeg:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    auth=auth,
                    headers={'Range': EXIF_RANGE},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status in (200, 206):
                        data = await resp.read()
                        
                        # File size fallback
                        if not meta.get('file_size'):
                            content_range = resp.headers.get('Content-Range', '')
                            if '/' in content_range:
                                try:
                                    meta['file_size'] = int(content_range.split('/')[-1])
                                except ValueError:
                                    pass

                        exif = piexif.load(data)
                        thumbnail_bytes = exif.get('thumbnail')
                        _extract_exif_meta(exif, meta)
        except Exception:
            pass

    # 3. Brightness score (always try if we don't have it yet)
    if not thumbnail_bytes and file_id:
        try:
            # Fetch a tiny preview for brightness calculation
            preview_url = f"{nextcloud_url.rstrip('/')}/index.php/core/preview?fileId={file_id}&x=32&y=32&a=1&forceIcon=0"
            async with aiohttp.ClientSession() as session:
                async with session.get(preview_url, auth=auth, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        thumbnail_bytes = await resp.read()
        except Exception:
            pass

    if thumbnail_bytes:
        try:
            img = Image.open(io.BytesIO(thumbnail_bytes)).convert('L')
            pixels = list(img.getdata())
            meta['brightness_score'] = round(sum(pixels) / len(pixels) / 255 * 100)
        except Exception:
            pass

    await set_cached_json(cache_key, meta, ttl=3600)  # Cache metadata for 1 hour
    return meta


def _extract_nc_meta(xml_content: bytes, meta: dict):
    ns = {'d': 'DAV:', 'oc': 'http://owncloud.org/ns', 'nc': 'http://nextcloud.org/ns'}
    try:
        tree = ElementTree.fromstring(xml_content)
        response = tree.find('d:response', ns)
        if response is None:
            return

        def _get(prop):
            return response.findtext(f'.//{prop}', namespaces=ns)

        # Basic file info
        size = _get('oc:size')
        if size: meta['file_size'] = int(size)

        # Dimensions
        w, h = _get('nc:width'), _get('nc:height')
        if w and h:
            meta['width'], meta['height'] = int(w), int(h)

        # Date
        ctime = _get('nc:creation_time')
        if ctime: meta['date_ts'] = int(ctime)

        # GPS
        lat, lon = _get('nc:latitude'), _get('nc:longitude')
        if lat and lon:
            meta['gps_lat'], meta['gps_lon'] = float(lat), float(lon)

        # Camera
        make, model = _get('nc:camera_make'), _get('nc:camera_model')
        if make: meta['camera_make'] = make
        if model: meta['camera_model'] = model

        # Exposure
        f_num = _get('nc:f_number')
        if f_num: meta['aperture'] = float(f_num)

        exp_time = _get('nc:exposure_time')
        if exp_time:
            # Nextcloud often returns exposure as a float string
            try:
                meta['shutter_speed'] = float(exp_time)
            except ValueError:
                pass

        iso = _get('nc:iso')
        if iso: meta['iso'] = int(iso)

        focal = _get('nc:focal_length')
        if focal: meta['focal_length'] = float(focal)

    except Exception:
        log.exception('Error parsing Nextcloud metadata XML')


def _extract_exif_meta(exif: dict, meta: dict):
    ifd0 = exif.get('0th', {})
    exif_ifd = exif.get('Exif', {})
    gps_ifd = exif.get('GPS', {})

    # Camera make / model
    raw_make = ifd0.get(piexif.ImageIFD.Make)
    raw_model = ifd0.get(piexif.ImageIFD.Model)
    if raw_make:
        meta['camera_make'] = raw_make.decode('utf-8', errors='ignore').strip('\x00 ')
    if raw_model:
        meta['camera_model'] = raw_model.decode('utf-8', errors='ignore').strip('\x00 ')

    # Capture date  (format: b'2024:04:10 14:30:00')
    raw_dt = exif_ifd.get(piexif.ExifIFD.DateTimeOriginal)
    if raw_dt:
        try:
            meta['date_ts'] = int(
                datetime.strptime(raw_dt.decode(), '%Y:%m:%d %H:%M:%S').timestamp()
            )
        except Exception:
            pass

    # Exposure triangle
    exp = exif_ifd.get(piexif.ExifIFD.ExposureTime)
    if exp and exp[1]:
        meta['shutter_speed'] = exp[0] / exp[1]

    fn = exif_ifd.get(piexif.ExifIFD.FNumber)
    if fn and fn[1]:
        meta['aperture'] = round(fn[0] / fn[1], 1)

    iso = exif_ifd.get(piexif.ExifIFD.ISOSpeedRatings)
    if iso is not None:
        meta['iso'] = iso

    fl = exif_ifd.get(piexif.ExifIFD.FocalLength)
    if fl and fl[1]:
        meta['focal_length'] = fl[0] / fl[1]

    # Pixel dimensions (Exif IFD is authoritative for JPEGs)
    px_w = exif_ifd.get(piexif.ExifIFD.PixelXDimension)
    px_h = exif_ifd.get(piexif.ExifIFD.PixelYDimension)
    if px_w and px_h:
        meta['width'] = px_w
        meta['height'] = px_h

    # GPS  (degrees/minutes/seconds rationals → decimal)
    if gps_ifd:
        try:
            lat_r = gps_ifd.get(piexif.GPSIFD.GPSLatitude)
            lon_r = gps_ifd.get(piexif.GPSIFD.GPSLongitude)
            lat_ref = (gps_ifd.get(piexif.GPSIFD.GPSLatitudeRef) or b'N').decode()
            lon_ref = (gps_ifd.get(piexif.GPSIFD.GPSLongitudeRef) or b'E').decode()
            if lat_r and lon_r:
                lat = _dms(lat_r) * (-1 if lat_ref == 'S' else 1)
                lon = _dms(lon_r) * (-1 if lon_ref == 'W' else 1)
                meta['gps_lat'] = lat
                meta['gps_lon'] = lon
        except Exception:
            pass


def _dms(rational_tuple) -> float:
    """Convert EXIF GPS (deg, min, sec) rationals to decimal degrees."""
    d, m, s = rational_tuple
    return d[0] / d[1] + m[0] / m[1] / 60 + s[0] / s[1] / 3600
