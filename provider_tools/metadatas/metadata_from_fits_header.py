import math

import dateutil

from .metadata import Metadata

__all__ = ['MetadataFromFitsHeader']


class MetadataFromFitsHeader(Metadata):
	"""Build metadata payloads from FITS header values."""

	# Methods to convert the FITS keywords values to the expected keyword type
	FIELD_VALUE_CONVERSION = {
		'text': str,
		'boolean': bool,
		'integer': int,
		'real': float,
		'time (ISO 8601)': dateutil.parser.parse,
	}

	def __init__(self, keywords, fits_header):
		"""Store the keyword definitions and FITS header."""
		super().__init__(keywords)
		self.fits_header = fits_header

	def extract_field_value(self, keyword):
		"""Extract a field value from the FITS header."""
		try:
			field_value = self.fits_header[keyword['verbose_name']]
		except KeyError:
			raise ValueError('Keyword %s missing from FITS header' % keyword['verbose_name'])

		field_value_conversion = self.FIELD_VALUE_CONVERSION.get(keyword['type'], None)

		if field_value_conversion is not None:
			try:
				field_value = field_value_conversion(field_value)
			except Exception as error:
				raise ValueError('Cannot convert value "%s" to %s' % (field_value, keyword['type'])) from error

		if isinstance(field_value, float) and not math.isfinite(field_value):
			field_value = None

		return field_value

	def get_field_oid(self):
		"""Return the observation identifier derived from the FITS header."""
		# By default use the date of observation as there is usually no more that 1 observation per second
		# Otherwise override
		observation_date = self.get_field_value('date_beg')
		if observation_date:
			return observation_date.strftime('%Y%m%d%H%M%S')
		else:
			raise ValueError('date_beg is not defined, cannot compute oid')

	def get_field_fits_header(self):
		"""Return the FITS header as a serialized string."""
		# Convert the fits_header to string for the metadata resources that require it
		return self.fits_header.tostring().strip()
