import json
import logging
from pprint import pformat

import requests

from ..data_locations import DataLocationFromUrl
from ..metadatas import MetadataFromFitsHeader
from ..utils import get_fits_header_from_url
from .extractor import Extractor

__all__ = ['ExtractorFromFitsUrl']


class ExtractorFromFitsUrl(Extractor):
	"""Extract metadata for FITS files referenced by URL."""

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
		"""Build a resource payload from a remote FITS file."""
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

	def write_metadata(self, file_urls, output_file):
		"""Write metadata and data_location payloads for FITS URLs."""

		for file_url in file_urls:
			logging.info('Extracting metadata and data_location resource for file "%s"', file_url)

			try:
				resource_data = self.get_resource_data(file_url)
			except Exception as error:
				logging.critical('Could not extract resource data for file "%s": %s', file_url, error)
				continue
			else:
				logging.debug(pformat(resource_data, indent=2, width=200))

			json.dump(resource_data, output_file, default=str)
			output_file.write('\n')
