import json
from pprint import pformat

import requests

from ..data_locations import DataLocationFromUrl
from ..metadatas import MetadataFromFitsHeader
from ..utils import get_fits_header_from_url
from .provider import Provider

__all__ = ['ProviderFromFitsUrl']


class ProviderFromFitsUrl(Provider):
	"""Submit metadata and data_location resources for remote FITS files.

	Downloads and parses the FITS header from a given URL, converts it
	into a metadata resource payload using ``METADATA_CLASS``, and pairs
	it with a data_location payload built from the URL using
	``DATA_LOCATION_CLASS``.
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

		Downloads only the FITS header (``HEADER_SIZE`` bytes starting at
		``HEADER_OFFSET``) from ``file_url``, parses it with
		``METADATA_CLASS``, and combines it with a data_location payload
		built by ``DATA_LOCATION_CLASS``.

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
		resource_data['data_location']['dataset'] = self.dataset['resource_uri']
		return resource_data

	def submit_new_metadata(self, file_urls):
		"""Extract metadata and data_location payloads for a list of FITS URLs and create the corresponding metadata and data_location resources.

		For each URL, builds the resource payload and
		then creates a new metadata resource.
		Errors for individual URLs are logged and do not stop processing
		of the remaining URLs.

		Args:
			file_urls (Iterable[str]): URLs of the FITS files to process.

		Returns:
			None
		"""
		for file_url in file_urls:
			self.logger.info('Processing URL "%s"', file_url)

			try:
				resource_data = self.get_resource_data(file_url)
			except Exception as error:
				self.logger.critical('Could not extract resource data for URL "%s": %s', file_url, error)
				continue
			else:
				self.logger.debug(pformat(resource_data, indent=2, width=200))

			try:
				result = self.create(resource_data)
			except Exception as error:
				self.logger.error('Could not create new metadata or data_location resource for URL "%s": %s', file_url, error)
			else:
				self.logger.info('Created new metadata resource "%s" for URL "%s"', result['resource_uri'], file_url)

	def write_metadata(self, file_urls, output_file):
		"""Extract metadata and data_location payloads for a list of FITS URLs and write them to a JSONL file.

		For each URL, builds the resource payload via :meth:`get_resource_data`
		and writes it as a single line of JSON to ``output_file`` (JSON Lines
		format). Errors for individual URLs are logged and skipped, allowing
		the remaining URLs to still be processed.

		Args:
			file_urls (Iterable[str]): URLs of the FITS files to process.
			output_file: A writable file-like object that JSON-serialized
				resource payloads are written to, one per line. Non-JSON-native
				values (e.g. dates) are serialized using ``str()`` as a fallback.

		Returns:
			None
		"""
		for file_url in file_urls:
			self.logger.info('Processing URL "%s"', file_url)

			try:
				resource_data = self.get_resource_data(file_url)
			except Exception as error:
				self.logger.critical('Could not extract resource data for URL "%s": %s', file_url, error)
				continue
			else:
				self.logger.debug(pformat(resource_data, indent=2, width=200))

			json.dump(resource_data, output_file, default=str)
			output_file.write('\n')
