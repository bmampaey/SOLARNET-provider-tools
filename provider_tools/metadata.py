import logging
from math import isfinite
from dateutil.parser import parse
from astropy import io, units, time

__all__ = ['MetadataFromFitsFile', 'MetadataFromTapRecord']

class MetadataFromFitsFile:
	'''Class to extract the metadata from a FITS file and generate the corresponding resource data for updating the SVO'''
	
	# Methods to convert the FITS keywords values to the proper SVO type
	KEYWORD_VALUE_CONVERSION = {
		'text': str,
		'boolean': bool,
		'integer': int,
		'real': float,
		'time (ISO 8601)': parse
	}
	
	# The HDU to read the fits header from
	DEFAULT_FITS_HDU = 0
	
	def __init__(self, fits_file = None, fits_hdu = None, oid = None, keywords = []):
		self.fits_file = fits_file
		self.fits_hdu = fits_hdu if fits_hdu is not None else self.DEFAULT_FITS_HDU
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
		
		with io.fits.open(self.fits_file) as hdus:
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
		
		keyword_value_conversion = self.KEYWORD_VALUE_CONVERSION.get(keyword['type'], None)
		
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
		'''Return the observation id (oid) for the resource. Override to adapt to the desired behavior'''
		if self.oid:
			return self.oid
		else:
			return self.get_field_value('date_beg').strftime('%Y%m%d%H%M%S')


class MetadataFromTapRecord:
	'''Class to extract the metadata from a TAPRecord'''
	
	# Methods to convert the TAPRecord values to the proper SVO type
	KEYWORD_VALUE_CONVERSION = {
		'text': str,
		'boolean': bool,
		'integer': int,
		'real': float,
		'time (ISO 8601)': parse
	}
	
	def __init__(self, record = None, oid = None, keywords = []):
		self.record = record
		self.oid = oid
		self.keywords = {keyword['name']: keyword for keyword in keywords}
	
	def get_resource_data(self):
		'''Return a dict of data for creating/updating a metadata resource'''
		
		# Create the resource data dict with the required keywords
		# and use the metadata keyword definitions to extract the value for the rest of the fields
		resource_data = {field: self.get_field_value(field) for field in ('oid', 'date_beg', 'date_end', 'wavemin', 'wavemax')}
		
		for name in self.keywords:
			try:
				resource_data[name] = self.get_field_value(name)
			except ValueError as why:
				logging.warning('Could not extract value for field %s: %s', name, why)
			else:
				logging.debug('Field %s has value "%s"', name, resource_data[name])
		
		return resource_data
	
	def get_field_value(self, field_name):
		'''Extract the metadata field value from the VO table row using the keyword definition'''
		
		# If there is a specific method to extract the value for the field, use it
		field_value_getter = getattr(self, 'get_field_' + field_name, None)
		if field_value_getter is not None:
			return field_value_getter()
		
		# Else extract the value from the record using the keyword verbose name and convert it following the keyword type
		try:
			keyword = self.keywords[field_name]
		except KeyError:
			raise ValueError('Keyword definition missing for field %s' % field_name)
		
		try:
			keyword_value = self.record[keyword['verbose_name']]
		except KeyError:
			raise ValueError('Field %s missing from TAPRecord' % keyword['verbose_name'])
		
		keyword_value_conversion = self.KEYWORD_VALUE_CONVERSION.get(keyword['type'], None)
		
		if keyword_value_conversion is not None:
			try:
				keyword_value = keyword_value_conversion(keyword_value)
			except Exception as why:
				raise ValueError('Could not convert value "%s" to %s' % (keyword_value, keyword['type'])) from why
		
		if keyword['type'] == 'real' and not isfinite(keyword_value):
			keyword_value = None
		
		return keyword_value
	
	def get_field_fits_header(self):
		'''Return the value of the fits_header metadata field'''
		return None
	
	def get_field_oid(self):
		'''Return the observation id (oid) for the resource. Override to adapt to the desired behavior'''
		if self.oid:
			return self.oid
		else:
			return self.get_field_value('granule_uid')
	
	def get_field_date_beg(self):
		return self.jd_to_datetime(self.get_field_value('time_min'))
	
	def get_field_date_end(self):
		return self.jd_to_datetime(self.get_field_value('time_max'))
	
	def get_field_wavemin(self):
		return self.hz_to_nm(self.get_field_value('spectral_range_min'))
	
	def get_field_wavemax(self):
		return self.hz_to_nm(self.get_field_value('spectral_range_max'))
	
	def hz_to_nm(self, value):
		'''Convert value in Hz to nm'''
		return (value * units.Hz).to(units.nm, equivalencies=units.spectral()).value
	
	def jd_to_datetime(self, value):
		'''Convert Julian Date to datetime'''
		return time.Time(value, format='jd').datetime
