function transform(data) {
  const m = data.metadata || {};

  const camera = m.camera_model
    ? (m.camera_make ? m.camera_make + ' ' + m.camera_model : m.camera_model)
    : null;

  const mpValue = (m.width && m.height) ? Math.round(m.width * m.height / 1e6) : 0;
  const megapixels = mpValue > 0 ? mpValue + ' MP' : null;

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
    image: {
      url:          data.image_url || null,
      path:         data.image_path || '',
      folder_count: data.folder_count || 0,
      seq_position: data.seq_position || null,
      bg_class:     _bgClass(m.brightness_score),
    },
    meta: {
      date_ts:    m.date_ts || null,
      filename:   data.image_path ? decodeURIComponent(data.image_path.split('/').pop()) : null,
      folder:     _parentFolder(data.image_path),
      camera:     camera,
      aperture:   aperture,
      shutter:    shutter,
      iso:        iso,
      exposure:   [aperture, shutter, iso].filter(Boolean).join('\u00A0· ') || null,
      focal:      focal,
      megapixels: megapixels,
      dimensions: dimensions,
      file_size:  fileSize,
      gps:        gps,
    },
    error: data.error || null,
  };
}

function _shutter(s) {
  if (s >= 1) return s + 's';
  return '1/' + Math.round(1 / s) + 's';
}

function _parentFolder(path) {
  if (!path) return null;
  const parts = path.split('/').filter(Boolean);
  if (parts.length < 2) return null;
  return decodeURIComponent(parts[parts.length - 2]);
}

function _fileSize(bytes) {
  if (bytes >= 1048576) return (bytes / 1048576).toFixed(1) + '\u00A0MB';
  return Math.round(bytes / 1024) + '\u00A0KB';
}

function _bgClass(score) {
  if (score == null) return null;
  const steps = [
    [7,  'bg--black'],
    [14, 'bg--gray-10'],
    [20, 'bg--gray-15'],
    [26, 'bg--gray-20'],
    [32, 'bg--gray-25'],
    [38, 'bg--gray-30'],
    [44, 'bg--gray-35'],
    [50, 'bg--gray-40'],
    [56, 'bg--gray-45'],
    [62, 'bg--gray-50'],
    [68, 'bg--gray-55'],
    [74, 'bg--gray-60'],
    [80, 'bg--gray-65'],
    [86, 'bg--gray-70'],
    [94, 'bg--gray-75'],
  ];
  let cls = 'bg--white';
  for (const [threshold, c] of steps) {
    if (score < threshold) {
      cls = c;
      break;
    }
  }
  return `2bit:${cls} 4bit:${cls}`;
}

function _gps(lat, lon) {
  const la = Math.abs(lat).toFixed(4) + '°' + (lat >= 0 ? 'N' : 'S');
  const lo = Math.abs(lon).toFixed(4) + '°' + (lon >= 0 ? 'E' : 'W');
  return la + '\u00A0' + lo;
}
