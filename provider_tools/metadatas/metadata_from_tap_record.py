from astropy import time, units

from .metadata import Metadata

__all__ = ['MetadataFromTapRecord']


class MetadataFromTapRecord(Metadata):
	"""Build metadata payloads from TAP records."""

	def __init__(self, keywords, tap_record):
		"""Store the keyword definitions and TAP record."""
		super().__init__(keywords)
		self.tap_record = tap_record

	def extract_field_value(self, keyword):
		"""Extract a field value from the TAP record."""
		try:
			return self.tap_record[keyword['verbose_name']]
		except KeyError:
			raise ValueError('Field %s missing from TAPRecord' % keyword['verbose_name'])

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
		return (value * units.Hz).to(units.nm, equivalencies=units.spectral()).value

	def jd_to_datetime(self, value):
		"""Convert a Julian date to a datetime object."""
		return time.Time(value, format='jd').datetime
