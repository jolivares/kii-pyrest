from kiicommon import BaseClient

class ServerCodeClient(BaseClient):
	def __init__(self, token):
		BaseClient.__init__(self, token)
	def deploy(self, filename):
		path = '/apps/%s/server-code' % (self._get_app())
		headers = {'Content-Type': 'application/javascript'}
		# open js file
		f = open(filename, "r")
		return self._send(path, 'POST', headers, f.read())['versionID']
	def get(self, version_id):
		path = '/apps/%s/server-code/versions/%s' % (self._get_app(), version_id)		
		return self._send(path, 'GET', {})
	def list(self):
		path = '/apps/%s/server-code/versions' % (self._get_app())		
		return self._send(path, 'GET', {})['versions']
	def delete(self, version_id):
		path = '/apps/%s/server-code/versions/%s' % (self._get_app(), version_id)		
		self._send(path, 'DELETE', {})
	def invoke(self, version_id, endpoint_name, args):
		path = '/apps/%s/server-code/versions/%s/%s' % (self._get_app(), version_id, endpoint_name)
		headers = {'Content-Type': 'application/json'}
		return self._send(path, 'POST', headers, args)