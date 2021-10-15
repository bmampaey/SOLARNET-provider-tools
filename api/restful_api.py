import json
from datetime import datetime
from functools import lru_cache
from slumber import API, serialize

__all__ = ['RESTfulApi']

# URL of the SVO RESTful API
API_URL = 'https://solarnet.oma.be/service/api/svo'

class RESTfulApi:
	'''RESTful API interface for the SVO'''
	def __init__(self, dataset_name, username, api_key):
		self.username = username
		self.api_key = api_key
		self.api = API(API_URL, auth=self.api_key_auth, serializer = serialize.Serializer(default = 'json', serializers = [JsonSerializer()]))
		
		try:
			self.dataset = self.api.dataset(dataset_name).get()
		except Exception as why:
			raise RuntimeError('Could not retrieve info for dataset "%s": %s', (dataset_name, why)) from why
		
		self.metadata_resource = getattr(self.api, self.dataset['metadata']['resource_uri'])
	
	def api_key_auth(self, request):
		'''Sets the API key authentication in the request header'''
		request.headers['Authorization'] = 'ApiKey %s:%s' % (self.username, self.api_key)
		return request
	
	@lru_cache()
	def get_keywords(self):
		'''Return the list of keywords for the dataset'''
		try:
			return self.api.keyword.get(dataset__name=self.dataset['name'], limit=0)['objects']
		except Exception as why:
			raise RuntimeError('Could not retrieve keywords for dataset "%s": %s' % (self.dataset['name'], why)) from why
	
	def create_metadata(self, metadata):
		'''Create a new metadata record for the dataset'''
		
		try:
			result = self.metadata_resource.post(metadata)
		except Exception as why:
			raise RuntimeError('Could not create metadata for dataset "%s": %s' % (self.dataset['name'], why.response.text if hasattr(why, 'response') else why)) from why
		return result
	
	def iter_metadata(self, **filters):
		'''Return an iterator of metadata records corresponding to the filters'''
		limit = 100
		offset = 0
		carry_on = True
		
		while carry_on:
			
			import ipdb; ipdb.set_trace()
			try:
				result = self.metadata_resource.get(**filters, limit=limit, offset=offset)
			except Exception as why:
				raise RuntimeError('Could not retrieve metadata %s for dataset "%s": %s' % (self.dataset['name'], why.response.text if hasattr(why, 'response') else why)) from why
			else:
				if result['meta']['next']:
					offset += limit
				else:
					carry_on = False
				for object in result['objects']:
					yield object
	
	def update_metadata(self, oid, metadata):
		'''Update an existing metadata record for the dataset'''
		try:
			result = self.metadata_resource(oid).patch(metadata)
		except Exception as why:
			raise RuntimeError('Could not update metadata %s for dataset "%s": %s' % (self.dataset['name'], oid, why.response.text if hasattr(why, 'response') else why)) from why
		return result
	
	def get_data_location(self, file_url):
		'''Return the data location corresponding to the file URL or None if no such exists'''
		try:
			data_locations = self.api.data_location.get(dataset__name = self.dataset['name'], file_url = file_url, limit = 1)['objects']
		except Exception as why:
			raise RuntimeError('Could not retrieve data location for dataset "%s": %s' % (self.dataset['name'], why.response.text if hasattr(why, 'response') else why)) from why
		
		return data_locations[0] if data_locations else None
	
	def create_data_location(self, data_location):
		'''Create a new data location record for the dataset'''
		try:
			result = self.api.data_location.post(data_location)
		except Exception as why:
			raise RuntimeError('Could not create data location for dataset "%s": %s' % (self.dataset['name'], why.response.text if hasattr(why, 'response') else why)) from why
		return result


class DateTimeEncoder(json.JSONEncoder):
	def default(self, o):
		if isinstance(o, datetime):
			return o.isoformat()
			
		return super().default(o)

class JsonSerializer(serialize.JsonSerializer):
	def dumps(self, data):
		return json.dumps(data, cls = DateTimeEncoder)
