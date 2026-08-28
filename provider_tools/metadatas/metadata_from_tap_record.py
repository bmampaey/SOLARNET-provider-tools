import astropy

from .metadata import Metadata

__all__ = ['MetadataFromTapRecord']


class MetadataFromTapRecord(Metadata):
	"""Build metadata payloads from an EPN-TAP record.

	Extracts field values by looking up each keyword's verbose name in the
	TAP record, converting the value to the type expected by the keyword,
	and providing dedicated handling for the required fields (identifier,
	observation dates, and wavelength range).
	"""

	def __init__(self, keywords, tap_record):
		"""Store the keyword definitions and TAP record to extract values from.

		Args:
			keywords (list): Keyword definitions describing the metadata
				fields to extract, as expected by :class:`Metadata`.
			tap_record (Mapping): The EPN-TAP record to extract field values from.
		"""
		super().__init__(keywords)
		self.tap_record = tap_record

	def extract_field_value(self, field_name):
		"""Extract and convert a field value from the TAP record.

		Args:
			field_name (str): The name of the resource field.

		Returns:
			(Any): The field value converted to the keyword type.

		Raises:
			KeyError: If no keyword correspond to teh field name.
			ValueError: If the field is missing from the TAP record, or if
				its value cannot be converted to the expected type.
		"""

		try:
			keyword = self.keywords[field_name]
		except KeyError as error:
			raise KeyError('No keyword correspond to field name %s' % field_name) from error

		try:
			field_value = self.tap_record[keyword['verbose_name']]
		except KeyError as error:
			raise ValueError('Field %s missing from TAP record' % keyword['verbose_name']) from error

		converted_field_value = self.convert_field_value(field_value, keyword['type'])

		return converted_field_value

	def get_oid(self):
		"""Return the unique observation identifier from the TAP record.

		Returns:
			(str): The value of the `granule_uid` field, which serves as the
				unique observation identifier in EPN-TAP.
		"""
		# In EPN-TAP the granule_id is the unique id of observation
		return self.extract_field_value('granule_uid')

	def get_date_beg(self):
		"""Return the observation start date from the TAP record.

		Returns:
			(datetime.datetime): The `time_min` field, converted from
				Julian date to a UTC datetime.
		"""
		# The time_min is in julian days, so convert it to UTC
		return self.jd_to_datetime(self.extract_field_value('time_min'))

	def get_date_end(self):
		"""Return the observation end date from the TAP record.

		Returns:
			(datetime.datetime): The `time_max` field, converted from
				Julian date to a UTC datetime.
		"""
		# The time_max is in julian days, so convert it to UTC
		return self.jd_to_datetime(self.extract_field_value('time_max'))

	def get_wavemin(self):
		"""Return the minimum wavelength of the observation, in nanometers.

		Returns:
			(float): The `spectral_range_max` field (in Hz), converted to
				nanometers and rounded to 2 decimal places. Note that the
				maximum frequency corresponds to the minimum wavelength.
		"""
		# The spectral_range_max is in Hz and the the wavemin is in nm, so the min in one unit is the max in the other
		return round(self.hz_to_nm(self.extract_field_value('spectral_range_max')), 2)

	def get_wavemax(self):
		"""Return the maximum wavelength of the observation, in nanometers.

		Returns:
			(float): The `spectral_range_min` field (in Hz), converted to
				nanometers and rounded to 2 decimal places. Note that the
				minimum frequency corresponds to the maximum wavelength.
		"""
		# The spectral_range_max is in Hz and the the wavemin is in nm, so the min in one unit is the max in the other
		return round(self.hz_to_nm(self.extract_field_value('spectral_range_min')), 2)

	def hz_to_nm(self, value):
		"""Convert a frequency value from hertz to a wavelength in nanometers.

		Args:
			value (float): Frequency value, in Hz.

		Returns:
			(float): The equivalent wavelength, in nanometers.
		"""
		return (value * astropy.units.Hz).to(astropy.units.nm, equivalencies=astropy.units.spectral()).value

	def jd_to_datetime(self, value):
		"""Convert a Julian date to a UTC datetime object.

		Args:
			value (float): Date expressed in Julian days.

		Returns:
			(datetime.datetime): The equivalent UTC datetime.
		"""
		return astropy.time.Time(value, format='jd').datetime
