import os
from urllib.parse import urljoin

from .data_location import DataLocation

__all__ = ['DataLocationFromLocalFile']


class DataLocationFromLocalFile(DataLocation):
	"""Build a data_location payload from a local file."""

	# The base file path to build the default file_path
	BASE_FILE_PATH = None

	# The base file URL to build the default file_url
	BASE_FILE_URL = None

	def __init__(self, local_file, **kwargs):
		"""Store the local file path and initialize the parent class."""
		self.local_file = local_file
		super().__init__(**kwargs)

	def get_file_url(self):
		"""Return the public URL for the local file."""
		if self.file_url is not None:
			return self.file_url
		elif self.BASE_FILE_URL:
			return urljoin(self.BASE_FILE_URL, self.get_file_path())
		else:
			raise ValueError('Either file_url or BASE_FILE_URL must be set')

	def get_file_size(self):
		"""Return the size of the local file in bytes."""
		if self.file_size is not None:
			return self.file_size
		elif self.local_file:
			return os.path.getsize(self.local_file)
		else:
			raise ValueError('Either file_size or local_file must be set')

	def get_file_path(self):
		"""Return the file path relative to the configured base path."""
		if self.file_path is not None:
			file_path = self.file_path
		elif self.local_file:
			file_path = self.local_file
			if self.BASE_FILE_PATH and os.path.abspath(file_path).startswith(self.BASE_FILE_PATH):
				file_path = os.path.abspath(file_path)[len(self.BASE_FILE_PATH) :]
		else:
			raise ValueError('Either file_path or local_file must be set')

		# file_path must always be relative
		return file_path.lstrip('./')
