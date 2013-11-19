import httplib2
import json

BASE_URL = ""
APP_ID = ""
APP_KEY = ""

def init_kii(appId, appKey, baseUrl):
	global BASE_URL
	BASE_URL = baseUrl

	global APP_ID
	APP_ID = appId

	global APP_KEY
	APP_KEY = appKey

def auth_app(client_id, client_secret):	
	uri = BASE_URL + '/oauth2/token'
	headers = {'X-Kii-AppId': APP_ID, 'X-Kii-AppKey': APP_KEY}
	headers['Content-Type'] = 'application/json'
	req = {'client_id': client_id, 'client_secret': client_secret}

	rest = httplib2.Http()
	res, content = rest.request(uri, 'POST', headers = headers, body = json.dumps(req))

	if res.status == 400:
		parse_auth_error(res, content)
	
	return json.loads(content)['access_token']

class BaseClient(object):
	def __init__(self, token):		
		self.token = token
	def _get_app(self):
		return APP_ID
	def _send(self, path, method, headers, body = {}):
		uri = BASE_URL + path
		headers['X-Kii-AppId'] = APP_ID
		headers['X-Kii-AppKey'] = APP_KEY
		headers['Authorization'] = 'Bearer ' + self.token

		data = json.dumps(body) if type(body) is dict else body		
		
		rest = httplib2.Http()
		res, content = rest.request(uri, method, headers = headers, body = data)

		print "Got server status %s" % res.status
		print "Got server response %s" % content

		if res.status > 399:
			parse_error(res, content)

		if res.status != 204 and content != None and 'json' in res.get('content-type', 'undefined'):			
			return json.loads(content)

		return content

def parse_error(res, content):
	print content
	error = "ERROR: "
	if content != None:
		if "json" in res['content-type']:
			e = json.loads(content)
			error += "%s - %s" % (e['errorCode'], e['message'])
		else:
			error += content
	raise Exception(res.status, error)

def parse_auth_error(res, content):
	error = "ERROR: "
	if content != None:
		if "json" in res['content-type']:
			e = json.loads(content)
			error += "%s - %s" % (e['errorCode'], e['error_description'])
		else:
			error += content
	raise Exception(res.status, error)
