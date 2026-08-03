__all__ = ['DataLocation']


class DataLocation:
	"""Base class for building data_location resource payloads."""

	def __init__(self, file_url=None, file_size=None, file_path=None, thumbnail_url=None, offline=False):
		"""Store the location metadata used to build a resource payload."""
		self.file_url = file_url
		self.file_size = file_size
		self.file_path = file_path
		self.thumbnail_url = thumbnail_url
		self.offline = offline

	def get_resource_data(self):
		"""Return a dictionary containing the data_location payload fields."""

		resource_data = {
			'file_url': self.get_file_url(),
			'file_size': self.get_file_size(),
			'file_path': self.get_file_path(),
			'thumbnail_url': self.get_thumbnail_url(),
			'offline': self.get_offline(),
		}

		return resource_data

	def get_file_url(self):
		"""Return the file URL for the resource."""
		return self.file_url

	def get_file_size(self):
		"""Return the file size in bytes."""
		return self.file_size

	def get_file_path(self):
		"""Return the relative file path."""
		# file_path must always be relative
		if self.file_path:
			return self.file_path.lstrip('./')

	def get_thumbnail_url(self):
		"""Return the thumbnail URL for the resource."""
		return self.thumbnail_url

	def get_offline(self):
		"""Return the offline flag value."""
		return self.offline
