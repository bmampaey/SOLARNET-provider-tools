import os
from urllib.parse import urljoin
import requests

__all__ = ['DataLocation', 'DataLocationFromLocalFile', 'DataLocationFromUrl', 'DataLocationFromTapRecord']


class DataLocation:
	'''Class to generate the resource data for updating the SVO'''
	
	def __init__(self, file_url = None, file_size = None, file_path = None, thumbnail_url = None, offline = False):
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
			'offline': self.get_offline(),
		}
		
		return resource_data
	
	def get_file_url(self):
		'''Override to return the proper URL for the file'''
		return self.file_url
	
	def get_file_size(self):
		'''Override to return the correct size of file in bytes'''
		return self.file_size
	
	def get_file_path(self):
		'''Override to return the proper relative file path for the file'''
		# file_path must always be relative
		return self.file_path.lstrip('./')
	
	def get_thumbnail_url(self):
		'''Override to return the proper URL for the thumbnail'''
		return self.thumbnail_url
	
	def get_offline(self):
		'''Override to return the proper value of the offline flag'''
		return self.offline


class DataLocationFromLocalFile(DataLocation):
	'''Class to inspect a local file and generate the corresponding resource data for updating the SVO'''
	
	# The base file path to build the default file_path
	BASE_FILE_PATH = None
	
	# The base file URL to build the default file_url
	BASE_FILE_URL = None
	
	def __init__(self, local_file, **kwargs):
		self.local_file = local_file
		super().__init__(**kwargs)
	
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
			if self.BASE_FILE_PATH and os.path.abspath(file_path).startswith(self.BASE_FILE_PATH):
				file_path = os.path.abspath(file_path)[len(self.BASE_FILE_PATH):]
		else:
			raise ValueError('Either file_path or local_file must be set')
		
		# file_path must always be relative
		return file_path.lstrip('./')


class DataLocationFromUrl(DataLocation):
	'''Class to inspect a URL and generate the corresponding resource data for updating the SVO'''
	
	# The base file URL to build the default file_path
	BASE_FILE_URL = None
	
	def __init__(self, file_url, **kwargs):
		super().__init__(file_url = file_url, **kwargs)
	
	def get_file_size(self):
		'''Override to return the correct size of file in bytes'''
		if self.file_size is not None:
			return self.file_size
		else:
			return int(requests.head(self.get_file_url()).headers['Content-length'])
	
	def get_file_path(self):
		'''Override to return the proper relative file path for the file'''
		if self.file_path is not None:
			file_path = self.file_path
		elif self.BASE_FILE_URL:
			file_path = self.get_file_url()[len(self.BASE_FILE_URL):]
		else:
			raise ValueError('Either file_path or BASE_FILE_URL must be set')
		
		# file_path must always be relative
		return file_path.lstrip('./')


class DataLocationFromTapRecord(DataLocation):
	'''Class to inspect a TAPRecord and generate the corresponding resource data for updating the SVO'''
	
	def __init__(self, record = None, **kwargs):
		self.record = record
		super().__init__(**kwargs)
	
	def get_file_url(self):
		'''Override to return the proper URL for the file'''
		if self.file_url is not None:
			return self.file_url
		elif 'access_url' in self.record:
			return self.record['access_url']
		else:
			raise ValueError('Either file_url must be set or access_url be defined on the record')
	
	def get_file_size(self):
		'''Override to return the correct size of file in bytes'''
		if self.file_size is not None:
			return self.file_size
		elif 'access_estsize' in self.record:
			return self.record['access_estsize']
		else:
			raise ValueError('Either file_size must be set or access_estsize be defined on the record')
	
	def get_file_path(self):
		'''Override to return the proper relative file path for the file'''
		if self.file_path is not None:
			file_path = self.file_path
		elif 'file_name' in self.record:
			file_path = self.record['file_name']
		else:
			raise ValueError('Either file_path must be set or file_name be defined on the record')
		
		# file_path must always be relative
		return file_path.lstrip('./')
	
	def get_thumbnail_url(self):
		'''Override to return the proper URL for the thumbnail'''
		if self.thumbnail_url is not None:
			return self.thumbnail_url
		elif 'thumbnail_url' in self.record:
			return self.record['thumbnail_url']
		else:
			raise ValueError('Either thumbnail_url must be set or thumbnail_url be defined on the record')
