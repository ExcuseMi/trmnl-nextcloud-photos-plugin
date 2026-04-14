function transform(data) {
  return {
    image_url: data.image_url || null,
    image_path: data.image_path || '',
    image_date: data.image_date || '',
    folder_count: data.folder_count || 0,
    error: data.error || null,
  };
}
