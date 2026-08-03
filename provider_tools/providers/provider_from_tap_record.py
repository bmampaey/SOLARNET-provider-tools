import logging
from pprint import pformat

from ..data_locations import DataLocationFromTapRecord
from ..metadatas import MetadataFromTapRecord
from .provider import Provider

__all__ = ['ProviderFromTapRecord']


class ProviderFromTapRecord(Provider):
	"""Submit metadata for TAP records through the API."""

	METADATA_CLASS = MetadataFromTapRecord

	DATA_LOCATION_CLASS = DataLocationFromTapRecord

	def get_resource_data(self, record):
		"""Build a resource payload from a TAP record."""
		metadata = self.METADATA_CLASS(tap_record=record, keywords=self.keywords)
		data_location = self.DATA_LOCATION_CLASS(record)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		resource_data['data_location']['dataset'] = self.dataset['resource_uri']
		return resource_data

	def submit_new_metadata(self, records, dry_run=False):
		"""Create metadata and data_location resources from TAP records."""

		for record in records:
			logging.info('Creating metadata and data_location resource for record "%s"', record)

			try:
				resource_data = self.get_resource_data(record)
			except Exception as error:
				logging.critical('Could not extract resource data for record "%s": %s', record, error)
			else:
				logging.debug(pformat(resource_data, indent=2, width=200))

				data_location = self.get_data_location(resource_data['data_location']['file_url'])
				if data_location is not None:
					logging.info('Data location for record %s already exists, reusing!', record)
					resource_data['data_location'] = data_location['resource_uri']

				if dry_run:
					logging.info('Called with dry-run option, not submitting anything')
				else:
					try:
						result = self.create(resource_data)
					except Exception as error:
						logging.error('Could not create new metadata or data_location resource for record "%s": %s', record, error)
					else:
						logging.info('Created new metadata resource "%s" for record "%s"', result['resource_uri'], record)
