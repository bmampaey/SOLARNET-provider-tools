import requests

from .data_location import DataLocation

__all__ = ['DataLocationFromUrl']


class DataLocationFromUrl(DataLocation):
	"""Build a data_location payload for a file hosted at a remote URL.

	The file size is fetched via an HTTP ``HEAD`` request when not
	explicitly provided, and the file path is derived by stripping
	``BASE_FILE_URL`` from the file URL when not explicitly provided.
	"""

	# The base file URL to build the default file_path
	BASE_FILE_URL = None

	def __init__(self, file_url, **kwargs):
		"""Initialize the data location from a remote file URL.

		Parameters
		----------
		file_url : str
		    Publicly accessible URL of the remote file.
		**kwargs
		    Additional keyword arguments forwarded to
		    :meth:`DataLocation.__init__` (``file_size``, ``file_path``,
		    ``thumbnail_url``, ``offline``).
		"""
		super().__init__(file_url=file_url, **kwargs)

	def get_file_size(self):
		"""Return the size of the remote file, in bytes.

		If ``self.file_size`` was not explicitly provided, it is
		determined by issuing an HTTP ``HEAD`` request to the file URL and
		reading the ``Content-Length`` response header.

		Returns
		-------
		int
		    The size of the file, in bytes.
		"""
		if self.file_size is not None:
			return self.file_size
		else:
			return int(requests.head(self.get_file_url()).headers['Content-length'])

	def get_file_path(self):
		"""Return the relative file path derived from the file URL.

		If ``self.file_path`` was not explicitly provided, it is computed
		by stripping the class-level ``BASE_FILE_URL`` prefix from the
		file URL.

		Returns
		-------
		str
		    The relative file path.

		Raises
		------
		ValueError
		    If neither ``self.file_path`` nor ``BASE_FILE_URL`` is set.
		"""
		if self.file_path is not None:
			file_path = self.file_path
		elif self.BASE_FILE_URL:
			file_path = self.get_file_url()[len(self.BASE_FILE_URL) :]
		else:
			raise ValueError('Either file_path or BASE_FILE_URL must be set')

		# file_path must always be relative
		return file_path.lstrip('./')
