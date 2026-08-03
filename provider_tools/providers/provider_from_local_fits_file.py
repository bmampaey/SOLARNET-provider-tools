import logging
from pprint import pformat

from ..data_locations import DataLocationFromLocalFile
from ..metadatas import MetadataFromFitsHeader
from ..utils import get_fits_header_from_local_file
from .provider import Provider

__all__ = ['ProviderFromLocalFitsFile']


class ProviderFromLocalFitsFile(Provider):
	"""Submit metadata for local FITS files through the API."""

	HDU_NAME_OR_INDEX = 0

	METADATA_CLASS = MetadataFromFitsHeader

	DATA_LOCATION_CLASS = DataLocationFromLocalFile

	def get_resource_data(self, file_path):
		"""Build a resource payload from a local FITS file."""
		metadata = self.METADATA_CLASS(
			fits_header=get_fits_header_from_local_file(file_path, self.HDU_NAME_OR_INDEX), keywords=self.keywords
		)
		data_location = self.DATA_LOCATION_CLASS(file_path)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		resource_data['data_location']['dataset'] = self.dataset['resource_uri']
		return resource_data

	def submit_new_metadata(self, file_paths, dry_run=False):
		"""Create metadata and data_location resources from local FITS files."""

		for file_path in file_paths:
			logging.info('Creating metadata and data_location resource for file "%s"', file_path)

			try:
				resource_data = self.get_resource_data(file_path)
			except Exception as error:
				logging.critical('Could not extract resource data for file "%s": %s', file_path, error)
			else:
				logging.debug(pformat(resource_data, indent=2, width=200))

				data_location = self.get_data_location(resource_data['data_location']['file_url'])

				if data_location is not None:
					logging.info('Data location for file %s already exists, reusing!', file_path)
					resource_data['data_location'] = data_location['resource_uri']

				if dry_run:
					logging.info('Called with dry-run option, not submitting anything')
				else:
					try:
						result = self.create(resource_data)
					except Exception as error:
						logging.error('Could not create new metadata or data_location resource for file "%s": %s', file_path, error)
					else:
						logging.info('Created new metadata resource "%s" for file "%s"', result['resource_uri'], file_path)
