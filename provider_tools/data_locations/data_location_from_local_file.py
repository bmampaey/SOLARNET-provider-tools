import os
from urllib.parse import urljoin

from .data_location import DataLocation

__all__ = ['DataLocationFromLocalFile']


class DataLocationFromLocalFile(DataLocation):
	"""Build a data_location payload from a file on the local filesystem.

	The relative file path is derived by stripping ``BASE_FILE_PATH``
	from the absolute file path ,and the file URL is derived by joining
	``BASE_FILE_URL`` with the relative	file path,
	and the file size is read from disk, when these values
	are not explicitly provided.
	"""

	# The base file path to build the default file_path
	BASE_FILE_PATH = None

	# The base file URL to build the default file_url
	BASE_FILE_URL = None

	def __init__(self, file_path, **kwargs):
		"""Initialize the data location from a local file path.

		Parameters
		----------
		file_path : str
		    Path to the file on the local filesystem.
		**kwargs
		    Additional keyword arguments forwarded to
		    :meth:`DataLocation.__init__` (``file_url``, ``file_size``,
		    ``thumbnail_url``, ``offline``).
		"""
		self.file_path = file_path
		super().__init__(**kwargs)

	def get_file_url(self):
		"""Return the public URL for the file.

		If ``self.file_url`` was not explicitly provided, it is built by
		joining the class-level ``BASE_FILE_URL`` with the relative file
		path.

		Returns
		-------
		str
		    The public URL of the file.

		Raises
		------
		ValueError
		    If neither ``self.file_url`` nor ``BASE_FILE_URL`` is set.
		"""
		if self.file_url is not None:
			return self.file_url
		elif self.BASE_FILE_URL:
			return urljoin(self.BASE_FILE_URL, self.get_file_path())
		else:
			raise ValueError('Either file_url or BASE_FILE_URL must be set')

	def get_file_size(self):
		"""Return the size of the local file, in bytes.

		If ``self.file_size`` was not explicitly provided, it is read
		from disk using ``os.path.getsize`` on ``self.file_path``.

		Returns
		-------
		int
		    The size of the file, in bytes.

		Raises
		------
		ValueError
		    If neither ``self.file_size`` nor ``self.file_path`` is set.
		"""
		if self.file_size is not None:
			return self.file_size
		elif self.file_path:
			return os.path.getsize(self.file_path)
		else:
			raise ValueError('Either file_size or file_path must be set')

	def get_file_path(self):
		"""Return the file path relative to the configured base path.

		If the absolute form of ``self.file_path`` starts with the
		class-level ``BASE_FILE_PATH``, that prefix is stripped so the
		resulting path is relative to it.

		Returns
		-------
		str
		    The relative file path.

		Raises
		------
		ValueError
		    If ``self.file_path`` is not set.
		"""
		if self.file_path is not None:
			file_path = self.file_path
		elif self.file_path:
			file_path = self.file_path
			if self.BASE_FILE_PATH and os.path.abspath(file_path).startswith(self.BASE_FILE_PATH):
				file_path = os.path.abspath(file_path)[len(self.BASE_FILE_PATH) :]
		else:
			raise ValueError('Either file_path or file_path must be set')

		# file_path must always be relative
		return file_path.lstrip('./')
