from ..data_locations import DataLocationFromLocalFile
from ..metadatas import MetadataFromFitsHeader
from ..utils import get_fits_header_from_local_file
from .provider import Provider

__all__ = ['ProviderFromLocalFitsFile']


class ProviderFromLocalFitsFile(Provider):
	"""Extract the metadata and data_location resource payloads for local FITS files.

	Attributes:
		HDU_NAME_OR_INDEX (str|int): HDU name or index from which to extract the metadata.
		METADATA_CLASS (MetadataFromFitsHeader): A subclass of Metadata used to build the metadata resource.
		DATA_LOCATION_CLASS (DataLocationFromLocalFile): A subclass of DataLocation used to build the data_location resource.
	"""

	HDU_NAME_OR_INDEX = 0

	METADATA_CLASS = MetadataFromFitsHeader

	DATA_LOCATION_CLASS = DataLocationFromLocalFile

	def get_resource_data(self, file_path):
		"""Build a metadata + data_location resource payload from a local FITS file.

		Args:
			file_path (str): Path to the local FITS file to read the header
				from and to register as the data location.

		Returns:
			(dict): A resource payload containing
				both the metadata fields and a nested `data_location` payload.

		Raises:
			Exception: Any error raised while fetching or parsing the FITS
				header is propagated to the caller.
		"""
		metadata = self.METADATA_CLASS(
			fits_header=get_fits_header_from_local_file(file_path, self.HDU_NAME_OR_INDEX), keywords=self.keywords
		)
		data_location = self.DATA_LOCATION_CLASS(file_path)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		return resource_data
