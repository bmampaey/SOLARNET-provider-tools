import yaml
from datetime import datetime, date, time
from http.client import HTTPConnection
from slumber import API, serialize
import simplejson as json


__all__ = ['RESTfulApi']

# URL of the SVO RESTful API
SVO_API_URL = 'https://solarnet.oma.be/service/api/svo'


class RESTfulApi(API):
	'''RESTful API interface for the SVO'''
	
	def __init__(self, username = None, api_key = None, auth_file = None, debug = False):
		# Get the username and API key from the auth file or the arguments
		# The auth_file always takes precedence
		if auth_file is not None:
			if username is not None or api_key is not None:
				raise ValueError('username and api_key cannot be specified if auth_file is specified')
			username, api_key = self.parse_auth_file(auth_file)
		
		# Override the auth to use the ApiKey authentication scheme
		if username is not None and api_key is not None:
			auth = ApiKeyAuth(username, api_key)
		else:
			auth = None
		
		# Override the serializer to accept datetime objects
		serializer = serialize.Serializer(default = 'json', serializers = [JsonSerializer()])
		
		super().__init__(base_url = SVO_API_URL, auth = auth, serializer = serializer)
		
		if debug:
			HTTPConnection.debuglevel = 1
	
	@classmethod
	def parse_auth_file(cls, auth_file):
		'''Read the username and api key from an auth file'''
		try:
			with open(auth_file, 'r') as file:
				auth = file.read().strip()
		except Exception as why:
			raise RuntimeError('Could not read SVO username and api key from file "%s": %s' % (auth_file, why)) from why
		
		try:
			username, api_key = auth.split(':', 1)
		except ValueError as why:
			raise RuntimeError('Auth file "%s" does not have the correct format, i.e. username:api_key' % auth_file) from why
		
		return username, api_key
	
	@classmethod
	def exception_to_text(cls, exception):
		text = str(exception)
		
		try:
			text += '\n' +  yaml.dump(exception.response.json())
		except Exception:
			pass
		else:
			return text
		
		try:
			text += '\n' + exception.response.text
		except Exception:
			pass
		else:
			return text
		
		return text
	
	def __call__(self, resource_uri):
		'''Returns a ressource by it's ressource URI'''
		return getattr(self, resource_uri)

class ApiKeyAuth:
	'''Set the API key authorization in the request header'''
	def __init__(self, username, api_key):
		self.username = username
		self.api_key = api_key
	
	def __call__(self, request):
		request.headers['Authorization'] = 'ApiKey %s:%s' % (self.username, self.api_key)
		return request

class JsonSerializer(serialize.JsonSerializer):
	'''JSON serialiser that accept datetime objects'''
	def dumps(self, data):
		return json.dumps(data, ignore_nan = True, cls = DateTimeEncoder)

class DateTimeEncoder(json.JSONEncoder):
	'''Encode a datetime object into an ISO 8601 sting'''
	def default(self, o):
		if isinstance(o, (datetime, date, time)):
			return o.isoformat()
			
		return super().default(o)
