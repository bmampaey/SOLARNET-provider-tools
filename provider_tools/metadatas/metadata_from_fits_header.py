import logging

from .metadata import Metadata

__all__ = ['MetadataFromFitsHeader']


class MetadataFromFitsHeader(Metadata):
	"""Build metadata payloads from FITS header values."""

	def __init__(self, keywords, fits_header):
		"""Store the keyword definitions and FITS header."""
		super().__init__(keywords)
		self.fits_header = fits_header

	def extract_field_value(self, keyword):
		"""Extract a metadata field value from the FITS header."""
		try:
			return self.fits_header[keyword['verbose_name']]
		except KeyError:
			raise ValueError('Keyword %s missing from FITS header' % keyword['verbose_name'])

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
