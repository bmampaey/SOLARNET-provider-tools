import os
from urllib.parse import urljoin

from .data_location import DataLocation

__all__ = ['DataLocationFromLocalFile']


class DataLocationFromLocalFile(DataLocation):
	"""Build a data_location payload from a local file.

	Class attributes:
		BASE_FILE_PATH: Base path used to derive the relative file path.
		BASE_FILE_URL: Base URL used to build the file URL.
	"""

	# The base file path to build the default file_path
	BASE_FILE_PATH = None

	# The base file URL to build the default file_url
	BASE_FILE_URL = None

	def __init__(self, local_file, **kwargs):
		"""Initialize the data location from a local file path.

		Args:
			local_file (str): Path to the file on the local filesystem.
			**kwargs: Additional keyword arguments forwarded to
				:meth:`DataLocation.__init__(file_url, file_size, file_path,
				thumbnail_url, offline) <DataLocation.__init__>`
		"""

		self.local_file = local_file
		super().__init__(**kwargs)

	def get_file_url(self):
		"""Return the public URL for the file.

		If ``self.file_url`` was not explicitly provided, it is built by
		joining the class-level ``BASE_FILE_URL`` with the relative file
		path.

		Returns
			str: The public URL of the file.

		Raises:
			ValueError: If neither ``self.file_url`` nor ``BASE_FILE_URL`` is set.
		"""

		if self.file_url is not None:
			return self.file_url
		elif self.BASE_FILE_URL:
			return urljoin(self.BASE_FILE_URL, self.get_file_path())
		else:
			raise ValueError('Either file_url or BASE_FILE_URL must be set')

	def get_file_size(self):
		"""Return the size of the file, in bytes.

		If ``self.file_size`` was not explicitly provided, it is read
		from disk using ``os.path.getsize`` on ``self.local_file``.

		Returns:
			int: The size of the file, in bytes.

		Raises:
			ValueError: If neither ``self.file_size`` nor ``self.local_file`` are set.
		"""

		if self.file_size is not None:
			return self.file_size
		elif self.local_file:
			return os.path.getsize(self.local_file)
		else:
			raise ValueError('Either file_size or local_file must be set')

	def get_file_path(self):
		"""Return the file path relative to the configured base path.

		If ``self.file_path`` was not explicitly provided,
		it is computed by removing the class-level ``BASE_FILE_PATH`` prefix
		from the absolute path of ``self.local_file``.

		Returns:
			str: The relative file path.

		Raises:
			ValueError: If neither ``self.file_path`` nor ``self.local_file`` are set.
		"""

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
