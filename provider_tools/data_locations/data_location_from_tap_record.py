from .data_location import DataLocation

__all__ = ['DataLocationFromTapRecord']


class DataLocationFromTapRecord(DataLocation):
	"""Build a data_location payload from an EPN-TAP record."""

	def __init__(self, tap_record, **kwargs):
		"""Initialize the data location from a TAP record.

		Args:
			tap_record (Mapping): A TAP record (or record-like mapping) potentially
				containing the fields `access_url`, `access_estsize`,
				`file_name`, and `thumbnail_url`.
			**kwargs (Any): Additional keyword arguments forwarded to
				[`DataLocation.__init__`][provider_tools.data_locations.DataLocation.__init__].
				Accepted keys include:

				* **file_url** `str`: Publicly accessible URL of the file.
				* **file_size** `int`: Size of the file, in bytes.
				* **file_path** `str`: Path of the file, relative to some base location.
				* **thumbnail_url** `str`: URL of a thumbnail image representing the file.
				* **offline** `bool`: Whether the file is not accessible to users.

		"""
		self.tap_record = tap_record
		super().__init__(**kwargs)

	def get_file_url(self):
		"""Return the public URL for the file.

		If `self.file_url` was not explicitly provided,
		it returns the TAP record's `access_url`.

		Returns:
			(str): The file URL.

		Raises:
			ValueError: If `self.file_url` is not set and the TAP record does
				not define `access_url`.
		"""
		if self.file_url is not None:
			return self.file_url
		elif 'access_url' in self.tap_record:
			return self.tap_record['access_url']
		else:
			raise ValueError('Either file_url must be set or access_url be defined on the record')

	def get_file_size(self):
		"""Return the size of the file, in bytes.

		If `self.file_size` was not explicitly provided,
		it returns the TAP record's `access_estsize`.

		Returns:
			(int): The estimated size of the file, in bytes.

		Raises:
			ValueError: If `self.file_size` is not set and the TAP record does
				not define `access_estsize`.
		"""
		if self.file_size is not None:
			return self.file_size
		elif 'access_estsize' in self.tap_record:
			return self.tap_record['access_estsize']
		else:
			raise ValueError('Either file_size must be set or access_estsize be defined on the record')

	def get_file_path(self):
		"""Return the relative file path.

		If `self.file_path` was not explicitly provided,
		it returns the TAP record's `file_name`.

		Returns:
			(str): The relative file path.

		Raises:
			ValueError: If `self.file_path` is not set and the TAP record does
				not define `file_name`.
		"""
		if self.file_path is not None:
			file_path = self.file_path
		elif 'file_name' in self.tap_record:
			file_path = self.tap_record['file_name']
		else:
			raise ValueError('Either file_path must be set or file_name be defined on the record')

		# file_path must always be relative
		return file_path.lstrip('./')

	def get_thumbnail_url(self):
		"""Returns the thumbnail URL, falling back to the TAP record's `thumbnail_url`.

		Returns:
			(str | None): The thumbnail URL, or `None` if neither `self.thumbnail_url`
				nor the TAP record's `thumbnail_url` field is available.
		"""
		if self.thumbnail_url is not None:
			return self.thumbnail_url
		elif 'thumbnail_url' in self.tap_record:
			return self.tap_record['thumbnail_url']
		else:
			return None
