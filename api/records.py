import os
import logging
from urllib.parse import urljoin
from dateutil.parser import parse, ParserError
from astropy.io import fits

__all__ = ['MetadataRecord', 'DataLocationRecord']

# Methods to convert the FITS keywords value to the proper SVO type
KEYWORD_VALUE_CONVERSION = {
	'text': str,
	'boolean': bool,
	'integer': int,
	'real': float,
	'time (ISO 8601)': parse
}

class MetadataRecord:
	'''Class to extract the metadata from a FITS file and generate the corresponding resource data for updating the SVO'''
	
	def __init__(self, fits_file = None, fits_hdu = 0, oid = None, keywords = []):
		self.fits_file = fits_file
		self.fits_hdu = fits_hdu
		self.oid = oid
		self.keywords = {keyword['name']: keyword for keyword in keywords}
		self.fits_header = self.get_fits_header()
	
	def get_resource_data(self):
		'''Return a dict of data for creating/updating a metadata resource'''
		
		# Create the resource data dict with the required keywords
		# and use the metadata keyword definitions to extract the value for the rest of the fields
		resource_data = {field: self.get_field_value(field) for field in ('oid', 'fits_header', 'date_beg', 'date_end', 'wavemin', 'wavemax')}
		
		for name in self.keywords:
			try:
				resource_data[name] = self.get_field_value(name)
			except ValueError as why:
				logging.warning('Could not extract value for field %s: %s', name, why)
			else:
				logging.debug('Field %s has value "%s"', name, resource_data[name])
		
		return resource_data
	
	def get_fits_header(self):
		'''Return the FITS header. Override this method if you don't read the file from disk or an URL'''
		
		with fits.open(self.fits_file) as hdus:
			fits_header = hdus[self.fits_hdu].header
		return fits_header
	
	def get_field_value(self, field_name):
		'''Extract the metadata field value from the FITS header using the keyword definition'''
		
		# If there is a specific method to extract the value for the field, use it
		field_value_getter = getattr(self, 'get_field_' + field_name, None)
		if field_value_getter is not None:
			return field_value_getter()
		
		# Else extract the value from the FITS header using the keyword verbose name and convert it following the keyword type
		try:
			keyword = self.keywords[field_name]
		except KeyError:
			raise ValueError('Keyword definition missing for field %s' % field_name)
		
		try:
			keyword_value = self.fits_header[keyword['verbose_name']]
		except KeyError:
			raise ValueError('Keyword %s missing from FITS header' % keyword['verbose_name'])
		
		keyword_value_conversion = KEYWORD_VALUE_CONVERSION.get(keyword['type'], None)
		
		if keyword_value_conversion is not None:
			try:
				keyword_value = keyword_value_conversion(keyword_value)
			except Exception as why:
				raise ValueError('Could not convert value "%s" to %s' % (keyword_value, keyword['type'])) from why
		
		return keyword_value
	
	def get_field_fits_header(self):
		'''Return the value of the fits_header metadata field'''
		return self.fits_header.tostring().strip()
	
	def get_field_oid(self):
		'''Return the observation id (oid) for the record. Override to adapt to the desired behavior'''
		if self.oid:
			return self.oid
		else:
			return self.get_field_value('date_beg').strftime('%Y%m%d%H%M%S')


class DataLocationRecord:
	'''Class to inspect a local file and generate the corresponding resource data for updating the SVO'''
	
	# The base directory to build the default file_path
	BASE_FILE_DIRECTORY = None
	
	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = None
	
	def __init__(self, local_file = None, file_url = None, file_size = None, file_path = None, thumbnail_url = None, offline = False):
		self.local_file = local_file
		self.file_url = file_url
		self.file_size = file_size
		self.file_path = file_path
		self.thumbnail_url = thumbnail_url
		self.offline = offline
	
	def get_resource_data(self):
		'''Return a dict of data for creating/updating a data_location resource'''
		
		resource_data = {
			'file_url': self.get_file_url(),
			'file_size': self.get_file_size(),
			'file_path': self.get_file_path(),
			'thumbnail_url': self.get_thumbnail_url(),
			'offline': self.offline,
		}
		
		return resource_data
	
	def get_file_url(self):
		'''Override to return the proper URL for the file'''
		if self.file_url is not None:
			return self.file_url
		elif self.BASE_FILE_URL:
			return urljoin(self.BASE_FILE_URL, self.get_file_path())
		else:
			raise ValueError('Either file_url or BASE_FILE_URL must be set')
	
	def get_file_size(self):
		'''Override to return the correct size of file in bytes'''
		if self.file_size is not None:
			return self.file_size
		elif self.local_file:
			return os.path.getsize(self.local_file)
		else:
			raise ValueError('Either file_size or local_file must be set')
	
	def get_file_path(self):
		'''Override to return the proper relative file path for the file'''
		if self.file_path is not None:
			file_path = self.file_path
		elif self.local_file:
			file_path = self.local_file
			if self.BASE_FILE_DIRECTORY and os.path.realpath(file_path).startswith(self.BASE_FILE_DIRECTORY):
				file_path = os.path.realpath(file_path)[len(self.BASE_FILE_DIRECTORY):]
		else:
			raise ValueError('Either file_path or local_file must be set')
		
		# file_path must always be relative
		return file_path.lstrip('./')
	
	def get_thumbnail_url(self):
		'''Override to return the proper URL for the thumbnail'''
		return self.thumbnail_url
