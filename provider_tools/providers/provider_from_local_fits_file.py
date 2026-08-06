import json
import pprint

from ..data_locations import DataLocationFromLocalFile
from ..metadatas import MetadataFromFitsHeader
from ..utils import get_fits_header_from_local_file
from .provider import Provider

__all__ = ['ProviderFromLocalFitsFile']


class ProviderFromLocalFitsFile(Provider):
	"""Submit metadata and data_location resources for local FITS files.

	Parses the header specified by ``HDU_NAME_OR_INDEX`` from a given FITS file path,
	converts it into a metadata resource payload using ``METADATA_CLASS``,
	and pairs it with a data_location payload built from the file using ``DATA_LOCATION_CLASS``.
	"""

	HDU_NAME_OR_INDEX = 0

	METADATA_CLASS = MetadataFromFitsHeader

	DATA_LOCATION_CLASS = DataLocationFromLocalFile

	def get_resource_data(self, file_path):
		"""Build a metadata + data_location resource payload from a local FITS file.

		Parses the header specified by ``HDU_NAME_OR_INDEX`` from a given FITS file path with
		``METADATA_CLASS``, and combines it with a data_location payload
		built by ``DATA_LOCATION_CLASS``.

		Args:
			file_path (str): Path to the local FITS file to read the header
				from and to register as the data location.

		Returns:
			dict: A resource payload suitable for :meth:`create`, containing
			both the metadata fields and a nested ``data_location`` payload.

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
