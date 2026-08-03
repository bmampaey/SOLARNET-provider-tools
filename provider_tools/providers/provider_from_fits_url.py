import logging
from pprint import pformat

from ..data_locations import DataLocationFromUrl
from ..metadatas import MetadataFromFitsHeader
from ..utils import get_fits_header_from_url
from .provider import Provider

__all__ = ['ProviderFromFitsUrl']


class ProviderFromFitsUrl(Provider):
	"""Submit metadata for FITS files referenced by URL."""

	# Must be a multiple of 2880
	HEADER_SIZE = 2880

	HEADER_OFFSET = 0

	ZIPPED = False

	WEBSERVER_AUTH = None

	METADATA_CLASS = MetadataFromFitsHeader

	DATA_LOCATION_CLASS = DataLocationFromUrl

	def get_resource_data(self, file_url):
		"""Build a resource payload from a remote FITS file."""
		metadata = self.METADATA_CLASS(
			fits_header=get_fits_header_from_url(
				file_url, self.HEADER_SIZE, self.HEADER_OFFSET, self.ZIPPED, self.WEBSERVER_AUTH
			),
			keywords=self.keywords,
		)
		data_location = self.DATA_LOCATION_CLASS(file_url)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		resource_data['data_location']['dataset'] = self.dataset['resource_uri']
		return resource_data

	def submit_new_metadata(self, file_urls, dry_run=False):
		"""Create metadata and data_location resources from FITS URLs."""

		for file_url in file_urls:
			logging.info('Creating metadata and data_location resource for URL "%s"', file_url)

			try:
				resource_data = self.get_resource_data(file_url)
			except Exception as error:
				logging.critical('Could not extract resource data for URL "%s": %s', file_url, error)
			else:
				logging.debug(pformat(resource_data, indent=2, width=200))

				data_location = self.get_data_location(resource_data['data_location']['file_url'])
				if data_location is not None:
					logging.info('Data location for URL %s already exists, reusing!', file_url)
					resource_data['data_location'] = data_location['resource_uri']

				if dry_run:
					logging.info('Called with dry-run option, not submitting anything')
				else:
					try:
						result = self.create(resource_data)
					except Exception as error:
						logging.error('Could not create new metadata or data_location resource for URL "%s": %s', file_url, error)
					else:
						logging.info('Created new metadata resource "%s" for URL "%s"', result['resource_uri'], file_url)
