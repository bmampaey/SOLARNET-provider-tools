from .data_location import DataLocation

__all__ = ['DataLocationFromTapRecord']


class DataLocationFromTapRecord(DataLocation):
	"""Build a data_location payload from a TAP record."""

	def __init__(self, tap_record, **kwargs):
		"""Store the TAP record and initialize the parent class."""
		self.tap_record = tap_record
		super().__init__(**kwargs)

	def get_file_url(self):
		"""Return the file URL declared by the TAP record."""
		if self.file_url is not None:
			return self.file_url
		elif 'access_url' in self.tap_record:
			return self.tap_record['access_url']
		else:
			raise ValueError('Either file_url must be set or access_url be defined on the record')

	def get_file_size(self):
		"""Return the estimated file size declared by the TAP record."""
		if self.file_size is not None:
			return self.file_size
		elif 'access_estsize' in self.tap_record:
			return self.tap_record['access_estsize']
		else:
			raise ValueError('Either file_size must be set or access_estsize be defined on the record')

	def get_file_path(self):
		"""Return the file path from the file name declared by the TAP record."""
		if self.file_path is not None:
			file_path = self.file_path
		elif 'file_name' in self.tap_record:
			file_path = self.tap_record['file_name']
		else:
			raise ValueError('Either file_path must be set or file_name be defined on the record')

		# file_path must always be relative
		return file_path.lstrip('./')

	def get_thumbnail_url(self):
		"""Return the thumbnail URL declared by the TAP record."""
		if self.thumbnail_url is not None:
			return self.thumbnail_url
		elif 'thumbnail_url' in self.tap_record:
			return self.tap_record['thumbnail_url']
		else:
			return None
