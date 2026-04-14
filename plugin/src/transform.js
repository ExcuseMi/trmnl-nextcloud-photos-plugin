function transform(data) {
  return {
    image_url: data.image_url || null,
    image_path: data.image_path || '',
  };
}
