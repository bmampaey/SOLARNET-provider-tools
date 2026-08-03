import math

import astropy
import dateutil

from .metadata import Metadata

__all__ = ['MetadataFromTapRecord']


class MetadataFromTapRecord(Metadata):
	"""Build metadata payloads from TAP records."""

	# Methods to convert the TAP record values to the expected keyword type
	FIELD_VALUE_CONVERSION = {
		'text': str,
		'boolean': bool,
		'integer': int,
		'real': float,
		'time (ISO 8601)': dateutil.parser.parse,
	}

	def __init__(self, keywords, tap_record):
		"""Store the keyword definitions and TAP record."""
		super().__init__(keywords)
		self.tap_record = tap_record

	def extract_field_value(self, keyword):
		"""Extract a field value from the TAP record."""
		try:
			field_value = self.tap_record[keyword['verbose_name']]
		except KeyError:
			raise ValueError('Field %s missing from TAP record' % keyword['verbose_name'])

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
		"""Return the unique observation identifier from the TAP record."""
		# In EPN-TAP the granule_id is the unique id of observation
		return self.tap_record['granule_uid']

	def get_field_date_beg(self):
		"""Return the start date of the observation from the TAP record."""
		# The time_min is in julian days, so convert it to UTC
		return self.jd_to_datetime(self.tap_record['time_min'])

	def get_field_date_end(self):
		"""Return the end date of the observation from the TAP record."""
		# The time_max is in julian days, so convert it to UTC
		return self.jd_to_datetime(self.tap_record['time_max'])

	def get_field_wavemin(self):
		"""Return the minimum wavelength of the observation from the TAP record."""
		# The spectral_range_max is in Hz and the the wavemin is in nm, so the min in one unit is the max in the other
		return round(self.hz_to_nm(self.tap_record['spectral_range_max']), 2)

	def get_field_wavemax(self):
		"""Return the maximum wavelength of the observation from the TAP record."""
		# The spectral_range_max is in Hz and the the wavemin is in nm, so the min in one unit is the max in the other
		return round(self.hz_to_nm(self.tap_record['spectral_range_min']), 2)

	def hz_to_nm(self, value):
		"""Convert a value from hertz to nanometers."""
		return (value * astropy.units.Hz).to(astropy.units.nm, equivalencies=astropy.units.spectral()).value

	def jd_to_datetime(self, value):
		"""Convert a Julian date to a datetime object."""
		return astropy.time.Time(value, format='jd').datetime
