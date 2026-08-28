from .metadata import Metadata

__all__ = ['MetadataFromFitsHeader']


class MetadataFromFitsHeader(Metadata):
	"""Build metadata payloads from FITS header values.

	Extracts field values by looking up each keyword's verbose name in the
	FITS header, converting the value to the type expected by the keyword,
	and providing dedicated handling for the observation identifier and
	the serialized header itself.
	"""

	def __init__(self, keywords, fits_header):
		"""Store the keyword definitions and FITS header to extract values from.

		Args:
			keywords (list): Keyword definitions describing the metadata
				fields to extract, as expected by :class:`Metadata`.
			fits_header (astropy.io.fits.Header): The FITS header (e.g. an
				`astropy.io.fits.Header`) to extract field values from.
		"""
		super().__init__(keywords)
		self.fits_header = fits_header

	def extract_field_value(self, field_name):
		"""Extract and convert a field value from the FITS header.

		Args:
			field_name (str): The name of the resource field.

		Returns:
			(Any): The field value converted to the keyword type.

		Raises:
			KeyError: If no keyword correspond to the field name.
			ValueError: If the keyword is missing from the FITS header, or
				if its value cannot be converted to the expected type.
		"""
		try:
			keyword = self.keywords[field_name]
		except KeyError as error:
			raise KeyError('No keyword correspond to field name %s' % field_name) from error

		try:
			field_value = self.fits_header[keyword['verbose_name']]
		except KeyError as error:
			raise ValueError('Keyword %s missing from FITS header' % keyword['verbose_name']) from error

		converted_field_value = self.convert_field_value(field_value, keyword['type'])

		return converted_field_value

	def get_oid(self):
		"""Return the observation identifier derived from the FITS header.

		By default, the observation start date is used to build the
		identifier, since there is usually no more than one observation per
		second. Subclasses handling data where this is not true should
		override this method.

		Returns:
			(str): The `date_beg` field formatted as `%Y%m%d%H%M%S`.

		Raises:
			ValueError: If `date_beg` could not be resolved, since the
				identifier cannot be computed without it.
		"""
		# By default use the date of observation as there is usually no more than 1 observation per second
		return self.get_date_beg().strftime('%Y%m%d%H%M%S')

	def get_date_beg(self):
		return self.extract_field_value('date_beg')

	def get_fits_header(self):
		"""Return the full FITS header serialized as a string.

		Returns:
			(str): The FITS header converted to its string representation,
				with leading/trailing whitespace stripped.
		"""
		# Convert the fits_header to string for the metadata resources that require it
		return self.fits_header.tostring().strip()
