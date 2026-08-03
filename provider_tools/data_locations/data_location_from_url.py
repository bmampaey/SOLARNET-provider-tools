import requests

from .data_location import DataLocation

__all__ = ['DataLocationFromUrl']


class DataLocationFromUrl(DataLocation):
	"""Build a data_location payload from a remote URL."""

	# The base file URL to build the default file_path
	BASE_FILE_URL = None

	def __init__(self, file_url, **kwargs):
		"""Store the remote file URL and initialize the parent class."""
		super().__init__(file_url=file_url, **kwargs)

	def get_file_size(self):
		"""Return the size of the remote file in bytes."""
		if self.file_size is not None:
			return self.file_size
		else:
			return int(requests.head(self.get_file_url()).headers['Content-length'])

	def get_file_path(self):
		"""Return the relative path derived from the configured BASE_FILE_URL."""
		if self.file_path is not None:
			file_path = self.file_path
		elif self.BASE_FILE_URL:
			file_path = self.get_file_url()[len(self.BASE_FILE_URL) :]
		else:
			raise ValueError('Either file_path or BASE_FILE_URL must be set')

		# file_path must always be relative
		return file_path.lstrip('./')
