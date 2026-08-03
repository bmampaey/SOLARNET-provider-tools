from .data_location import DataLocation
from .data_location_from_local_file import DataLocationFromLocalFile
from .data_location_from_tap_record import DataLocationFromTapRecord
from .data_location_from_url import DataLocationFromUrl

__all__ = [
	'DataLocation',
	'DataLocationFromLocalFile',
	'DataLocationFromTapRecord',
	'DataLocationFromUrl',
]
