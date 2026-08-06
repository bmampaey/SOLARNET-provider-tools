import json
import pprint

from ..data_locations import DataLocationFromTapRecord
from ..metadatas import MetadataFromTapRecord
from .provider import Provider

__all__ = ['ProviderFromTapRecord']


class ProviderFromTapRecord(Provider):
	"""Submit metadata and data_location resources for TAP records.

	Fetch and parses the TAP record from an EPN-TAP service, converts it
	into a metadata resource payload using ``METADATA_CLASS``, and pairs
	it with a data_location payload built from the URL using
	``DATA_LOCATION_CLASS``.
	"""

	METADATA_CLASS = MetadataFromTapRecord

	DATA_LOCATION_CLASS = DataLocationFromTapRecord

	def get_resource_data(self, tap_record):
		"""Build a metadata + data_location resource payload from a TAP record.

		Args:
			tap_record (dict): Values of the table record containing at least the keys
			granule_uid, time_min, time_max, spectral_range_min, spectral_range_max

		Returns:
			dict: A resource payload suitable for :meth:`create`, containing
			both the metadata fields and a nested ``data_location`` payload.

		Raises:
			Exception: Any error raised while fetching or parsing the FITS
				header is propagated to the caller.
		"""
		metadata = self.METADATA_CLASS(tap_record=tap_record, keywords=self.keywords)
		data_location = self.DATA_LOCATION_CLASS(tap_record)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		return resource_data
