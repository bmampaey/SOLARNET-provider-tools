import http.client

import slumber
import yaml

from .utils import JsonSerializer

__all__ = ['RESTfulApi']

# URL of the SVO RESTful API
SVO_API_URL = 'https://solarnet.oma.be/service/api/svo'


class RESTfulApi(slumber.API):
	"""RESTful API interface for the SVO.

	Args:
		username (str, optional): SVO username. Cannot be specified when
			``auth_file`` is specified.
		api_key (str, optional): SVO API key. Cannot be specified when
			``auth_file`` is specified.
		auth_file (str, optional): Path to a file containing the SVO username
			and API key in ``username:api_key`` format. Cannot be specified when
			``username`` and ``api_key`` are specified.
		debug (bool, optional): Whether to enable HTTP connection debugging.
			Defaults to ``False``.
	"""

	def __init__(self, username=None, api_key=None, auth_file=None, debug=False):
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
		serializer = slumber.serialize.Serializer(default='json', serializers=[JsonSerializer()])

		super().__init__(base_url=SVO_API_URL, auth=auth, serializer=serializer)

		if debug:
			# Carefull this modifies the behavior of the library
			http.client.HTTPConnection.debuglevel = 1

	@classmethod
	def parse_auth_file(cls, auth_file):
		"""Read the username and API key from an authentication file.

		Args:
			auth_file (str): Path to the authentication file.

		Returns:
			tuple[str, str]: A tuple containing the username and API key.

		Raises:
			RuntimeError: If the authentication file cannot be read or does
				not have the expected ``username:api_key`` format.
		"""
		try:
			with open(auth_file, 'r') as file:
				auth = file.read().strip()
		except OSError as error:
			raise RuntimeError('Could not read SVO username and api key from file "%s": %s' % (auth_file, error)) from error

		try:
			username, api_key = auth.split(':', 1)
		except ValueError as error:
			raise RuntimeError('Auth file "%s" does not have the correct format, i.e. username:api_key' % auth_file) from error

		return username, api_key

	@classmethod
	def exception_to_text(cls, exception):
		"""Convert an exception and its HTTP response to a text representation.

		Args:
			exception (Exception): Exception whose message and HTTP response
				should be converted to text.

		Returns:
			str: The exception message, optionally followed by the response
				JSON or text content.
		"""
		text = str(exception)

		try:
			text += '\n' + yaml.dump(exception.response.json())
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
		"""Return a resource by its resource URI.

		Args:
			resource_uri (str): URI identifying the resource to retrieve.

		Returns:
			object: The resource corresponding to ``resource_uri``.
		"""
		return getattr(self, resource_uri)


class ApiKeyAuth:
	"""Set the API key authorization in the request header"""

	def __init__(self, username, api_key):
		self.username = username
		self.api_key = api_key

	def __call__(self, request):
		request.headers['Authorization'] = 'ApiKey %s:%s' % (self.username, self.api_key)
		return request
