import logging
from datetime import datetime
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree

import aiohttp
import piexif

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
    """Read EXIF metadata directly from the file via a 64 KB range request.

    Works regardless of whether Nextcloud's Photos indexer has run.
    Non-JPEG files (PNG, WebP, …) silently return only file_size.
    """
    url = _dav_url(nextcloud_url, username, file_path)
    auth = aiohttp.BasicAuth(username, app_token)

    async with aiohttp.ClientSession() as session:
        async with session.get(
            url,
            auth=auth,
            headers={'Range': EXIF_RANGE},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status not in (200, 206):
                log.warning('Metadata range GET returned %s for %s', resp.status, file_path)
                return {}
            data = await resp.read()
            # Content-Range: bytes 0-65535/4821532  →  total file size
            content_range = resp.headers.get('Content-Range', '')

    meta: dict = {}

    # File size from Content-Range header
    if '/' in content_range:
        try:
            meta['file_size'] = int(content_range.split('/')[-1])
        except ValueError:
            pass

    # EXIF — JPEG/TIFF only; skip silently for other formats
    ext = file_path.rsplit('.', 1)[-1].lower() if '.' in file_path else ''
    if ext not in ('jpg', 'jpeg', 'tif', 'tiff'):
        return meta

    try:
        exif = piexif.load(data)
    except Exception:
        return meta

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

    return meta


def _dms(rational_tuple) -> float:
    """Convert EXIF GPS (deg, min, sec) rationals to decimal degrees."""
    d, m, s = rational_tuple
    return d[0] / d[1] + m[0] / m[1] / 60 + s[0] / s[1] / 3600
