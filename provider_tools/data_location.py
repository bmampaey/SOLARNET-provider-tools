import os
from urllib.parse import urljoin

__all__ = ['DataLocationFromLocalFile']

class DataLocationFromLocalFile:
	'''Class to inspect a local file and generate the corresponding resource data for updating the SVO'''
	
	# The base directory to build the default file_path
	BASE_FILE_DIRECTORY = None
	
	# The base file URL to build the default file_url
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
