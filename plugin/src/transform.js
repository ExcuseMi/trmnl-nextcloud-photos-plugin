function transform(data) {
  const m = data.metadata || {};

  const camera = m.camera_model
    ? (m.camera_make ? m.camera_make + ' ' + m.camera_model : m.camera_model)
    : null;

  const megapixels = (m.width && m.height)
    ? Math.round(m.width * m.height / 1e6) + ' MP'
    : null;

  const dimensions = (m.width && m.height)
    ? m.width + '×' + m.height
    : null;

  const aperture = m.aperture != null ? 'f/' + m.aperture : null;
  const shutter  = m.shutter_speed != null ? _shutter(m.shutter_speed) : null;
  const iso      = m.iso != null ? 'ISO\u00A0' + m.iso : null;
  const focal    = m.focal_length != null ? Math.round(m.focal_length) + 'mm' : null;

  const fileSize = m.file_size != null ? _fileSize(m.file_size) : null;

  const gps = m.location
    || ((m.gps_lat != null && m.gps_lon != null) ? _gps(m.gps_lat, m.gps_lon) : null);

  return {
    image_url:    data.image_url || null,
    image_path:   data.image_path || '',
    folder_count: data.folder_count || 0,
    error:        data.error || null,
    // individual metadata fields — null when unavailable
    meta_date_ts:   m.date_ts || null,
    meta_camera:    camera,
    meta_aperture:  aperture,
    meta_shutter:   shutter,
    meta_iso:       iso,
    meta_exposure:  [aperture, shutter, iso].filter(Boolean).join('\u00A0· ') || null,
    meta_focal:     focal,
    meta_megapixels: megapixels,
    meta_dimensions: dimensions,
    meta_file_size:  fileSize,
    meta_gps:        gps,
  };
}

function _shutter(s) {
  if (s >= 1) return s + 's';
  return '1/' + Math.round(1 / s) + 's';
}

function _fileSize(bytes) {
  if (bytes >= 1048576) return (bytes / 1048576).toFixed(1) + '\u00A0MB';
  return Math.round(bytes / 1024) + '\u00A0KB';
}

function _gps(lat, lon) {
  const la = Math.abs(lat).toFixed(4) + '°' + (lat >= 0 ? 'N' : 'S');
  const lo = Math.abs(lon).toFixed(4) + '°' + (lon >= 0 ? 'E' : 'W');
  return la + '\u00A0' + lo;
}
