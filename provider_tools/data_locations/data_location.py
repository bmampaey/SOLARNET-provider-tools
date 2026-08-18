__all__ = ['DataLocation']


class DataLocation:
	"""Base class for building data_location resource payloads.

	Subclasses are expected to override the getter methods of the payload fields
	(``file_url``, ``file_size``, ``file_path``, ``thumbnail_url``, ``offline``)
	for the specific data source (e.g. a remote URL, a TAP record, or a local file).
	"""

	def __init__(self, file_url=None, file_size=None, file_path=None, thumbnail_url=None, offline=False):
		"""Initialize the data_location with explicit values.

		Any of these values may be left as ``None`` (or ``False`` for
		``offline``) and computed lazily by subclasses that override the
		corresponding getter method.

		Args:
			file_url (str, optional): Publicly accessible URL of the file.
			file_size (int, optional): Size of the file, in bytes.
			file_path (str, optional): Path of the file, relative to some base location.
			thumbnail_url (str, optional): URL of a thumbnail image representing the file.
			offline (bool, optional): Whether the resource is only available offline.
				Defaults to ``False``.
		"""
		self.file_url = file_url
		self.file_size = file_size
		self.file_path = file_path
		self.thumbnail_url = thumbnail_url
		self.offline = offline

	def get_resource_data(self):
		"""Build the data_location resourcepayload.

		Returns:
			dict: Dictionary with the keys ``file_url``, ``file_size``,
			``file_path``, ``thumbnail_url``, and ``offline``.
		"""
		resource_data = {
			'file_url': self.get_file_url(),
			'file_size': self.get_file_size(),
			'file_path': self.get_file_path(),
			'thumbnail_url': self.get_thumbnail_url(),
			'offline': self.get_offline(),
		}

		return resource_data

	def get_file_url(self):
		"""Return the public URL of the file.

		Returns:
			str or None: The value of ``self.file_url``.
		"""
		return self.file_url

	def get_file_size(self):
		"""Return the size of the file, in bytes.

		Returns:
			int or None: The value of ``self.file_size``.
		"""
		return self.file_size

	def get_file_path(self):
		"""Return the file path, relative to its base location.

		Any leading ``/`` (or ``.``) characters are stripped so the
		returned path is always relative.

		Returns:
			str or None: The relative file path, or ``None`` if ``self.file_path`` is
			not set.
		"""
		# file_path must always be relative
		if self.file_path:
			return self.file_path.lstrip('./')

	def get_thumbnail_url(self):
		"""Return the URL of the thumbnail image for the resource.

		Returns:
			str or None: The value of ``self.thumbnail_url``.
		"""
		return self.thumbnail_url

	def get_offline(self):
		"""Return whether the resource is only available offline.

		Returns:
			bool: The value of ``self.offline``.
		"""
		return self.offline
