from kiicommon import BaseClient
from query import Query

class ExecutionQuery(Query):
	def to_map(self):
		return self._to_map('scheduleExecutionQuery')

class ExecutionClient(BaseClient):
	def __init__(self, token):
		BaseClient.__init__(self, token)
	def get(self, version_id):
		path = '/apps/%s/hooks/executions%s' % (self._get_app(), execution_id)
		return self._send(path, 'GET', {})
	def query(self, q):
		path = '/apps/%s/hooks/executions/query' % (self._get_app())
		headers = {'Content-Type': 'application/vnd.kii.ScheduleExecutionQueryRequest+json'}
		data = q.to_map()
		res = self._send(path, 'POST', headers, data)
		return res['results'], res['nextPaginationKey'] if 'nextPaginationKey' in res else None