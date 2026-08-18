import requests

from ..data_locations import DataLocationFromUrl
from ..metadatas import MetadataFromFitsHeader
from ..utils import get_fits_header_from_url
from .provider import Provider

__all__ = ['ProviderFromFitsUrl']


class ProviderFromFitsUrl(Provider):
	"""Extract the metadata and data_location resource payloads for remote FITS files.

	Class attributes:
		HEADER_SIZE: Size of the FITS header to read.
		HEADER_OFFSET: Byte offset from which to read the FITS header.
		ZIPPED: Whether the FITS file is compressed.
		WEBSERVER_AUTH: Authentication credentials for the web server.
		METADATA_CLASS: Class used to build the metadata resource.
		DATA_LOCATION_CLASS: Class used to build the data_location resource.
	"""

	# Must be a multiple of 2880
	HEADER_SIZE = 2880

	HEADER_OFFSET = 0

	ZIPPED = False

	WEBSERVER_AUTH = None

	METADATA_CLASS = MetadataFromFitsHeader

	DATA_LOCATION_CLASS = DataLocationFromUrl

	def __init__(self, *args, **kwargs):
		"""Initialize the extractor and its HTTP session."""
		super().__init__(*args, **kwargs)
		self.http_session = requests.Session()
		self.http_session.auth = self.WEBSERVER_AUTH

	def get_resource_data(self, file_url):
		"""Build a metadata + data_location resource payload from a FITS URL.

		Args:
			file_url (str): URL of the remote FITS file to read the header
				from and to register as the data location.

		Returns:
			dict: A resource payload suitable for :meth:`create`, containing
			both the metadata fields and a nested ``data_location`` payload.

		Raises:
			Exception: Any error raised while fetching or parsing the FITS
				header is propagated to the caller.
		"""

		metadata = self.METADATA_CLASS(
			fits_header=get_fits_header_from_url(
				file_url,
				self.http_session,
				self.HEADER_SIZE,
				self.HEADER_OFFSET,
				self.ZIPPED,
			),
			keywords=self.keywords,
		)
		data_location = self.DATA_LOCATION_CLASS(file_url)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		return resource_data
