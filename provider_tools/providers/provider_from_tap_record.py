from ..data_locations import DataLocationFromTapRecord
from ..metadatas import MetadataFromTapRecord
from .provider import Provider

__all__ = ['ProviderFromTapRecord']


class ProviderFromTapRecord(Provider):
	"""Extract the metadata and data_location resource payloads for TAP records.

	Attributes:
		METADATA_CLASS (MetadataFromTapRecord): A subclass of Metadata used to build the metadata resource.
		DATA_LOCATION_CLASS (DataLocationFromTapRecord): A subclass of DataLocation used to build the data_location resource.
	"""

	METADATA_CLASS = MetadataFromTapRecord

	DATA_LOCATION_CLASS = DataLocationFromTapRecord

	def get_resource_data(self, tap_record):
		"""Build a metadata + data_location resource payload from a TAP record.

		Args:
			tap_record (Mapping): Values of the table record containing at least the keys
				`granule_uid`, `time_min`, `time_max`, `spectral_range_min`, `spectral_range_max`

		Returns:
			(dict): A resource payload containing
				both the metadata fields and a nested `data_location` payload.

		Raises:
			Exception: Any error raised while fetching or parsing the FITS
				header is propagated to the caller.
		"""
		metadata = self.METADATA_CLASS(tap_record=tap_record, keywords=self.keywords)
		data_location = self.DATA_LOCATION_CLASS(tap_record)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		return resource_data
