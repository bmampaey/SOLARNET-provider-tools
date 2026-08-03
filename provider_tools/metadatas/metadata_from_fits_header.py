import math

import dateutil

from .metadata import Metadata

__all__ = ['MetadataFromFitsHeader']


class MetadataFromFitsHeader(Metadata):
	"""Build metadata payloads from FITS header values.

	Extracts field values by looking up each keyword's verbose name in the
	FITS header, converting the value to the type expected by the keyword,
	and providing dedicated handling for the observation identifier and
	the serialized header itself.
	"""

	# Methods to convert the FITS keywords values to the expected keyword type
	FIELD_VALUE_CONVERSION = {
		'text': str,
		'boolean': bool,
		'integer': int,
		'real': float,
		'time (ISO 8601)': dateutil.parser.parse,
	}

	def __init__(self, keywords, fits_header):
		"""Store the keyword definitions and FITS header to extract values from.

		Args:
			keywords (list): Keyword definitions describing the metadata
				fields to extract, as expected by :class:`Metadata`.
			fits_header: The FITS header (e.g. an
				``astropy.io.fits.Header``) to extract field values from.
		"""
		super().__init__(keywords)
		self.fits_header = fits_header

	def extract_field_value(self, keyword):
		"""Extract and convert a field value from the FITS header.

		Args:
			keyword (dict): Keyword definition with ``verbose_name`` (the
				key to look up in the FITS header) and ``type`` (used to
				select the appropriate conversion via
				:data:`FIELD_VALUE_CONVERSION`).

		Returns:
			The converted field value, or ``None`` if it converts to a
			non-finite float.

		Raises:
			ValueError: If the keyword is missing from the FITS header, or
				if its value cannot be converted to the expected type.
		"""
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
		"""Return the observation identifier derived from the FITS header.

		By default, the observation start date is used to build the
		identifier, since there is usually no more than one observation per
		second. Subclasses handling data where this is not true should
		override this method.

		Returns:
			str: The ``date_beg`` field formatted as ``%Y%m%d%H%M%S``.

		Raises:
			ValueError: If ``date_beg`` could not be resolved, since the
				identifier cannot be computed without it.
		"""
		# By default use the date of observation as there is usually no more that 1 observation per second
		# Otherwise override
		observation_date = self.get_field_value('date_beg')
		if observation_date:
			return observation_date.strftime('%Y%m%d%H%M%S')
		else:
			raise ValueError('date_beg is not defined, cannot compute oid')

	def get_field_fits_header(self):
		"""Return the full FITS header serialized as a string.

		Returns:
			str: The FITS header converted to its string representation,
			with leading/trailing whitespace stripped.
		"""
		# Convert the fits_header to string for the metadata resources that require it
		return self.fits_header.tostring().strip()
