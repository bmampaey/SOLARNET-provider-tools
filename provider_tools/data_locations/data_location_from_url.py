import requests

from .data_location import DataLocation

__all__ = ['DataLocationFromUrl']


class DataLocationFromUrl(DataLocation):
	"""Build a data_location payload for a file hosted at a remote URL.

	Attributes:
		BASE_FILE_URL (str): Base URL to derive the relative file path.
	"""

	# The base file URL to build the default file_path
	BASE_FILE_URL = None

	def __init__(self, file_url, **kwargs):
		"""Initialize the data location from a remote file URL.

		Args:
			file_url (str): Publicly accessible URL of the remote file.
			**kwargs (Any): Additional keyword arguments forwarded to
				[`DataLocation.__init__`][provider_tools.data_locations.DataLocation.__init__].
				Accepted keys include:

				* **file_url** `str`: Publicly accessible URL of the file.
				* **file_size** `int`: Size of the file, in bytes.
				* **file_path** `str`: Path of the file, relative to some base location.
				* **thumbnail_url** `str`: URL of a thumbnail image representing the file.
				* **offline** `bool`: Whether the file is not accessible to users.
		"""
		super().__init__(file_url=file_url, **kwargs)

	def get_file_size(self):
		"""Return the size of the file, in bytes.

		If `self.file_size` was not explicitly provided, it is
		determined by issuing an HTTP `HEAD` request to the file URL and
		reading the `Content-Length` response header.

		Returns:
			(int): The size of the file, in bytes.
		"""
		if self.file_size is not None:
			return self.file_size
		else:
			return int(requests.head(self.get_file_url()).headers['Content-length'])

	def get_file_path(self):
		"""Return the relative file path derived from the file URL.

		If `self.file_path` was not explicitly provided, it is computed
		by stripping the class-level `BASE_FILE_URL` prefix from the
		file URL.

		Returns:
			(str): The relative file path.

		Raises:
			ValueError: If neither `self.file_path` nor `BASE_FILE_URL` is set.
		"""
		if self.file_path is not None:
			file_path = self.file_path
		elif self.BASE_FILE_URL:
			file_path = self.get_file_url()[len(self.BASE_FILE_URL) :]
		else:
			raise ValueError('Either file_path or BASE_FILE_URL must be set')

		# file_path must always be relative
		return file_path.lstrip('./')
