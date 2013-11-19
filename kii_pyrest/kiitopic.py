from kiicommon import BaseClient

class TopicClient(BaseClient):	
	def __init__(self, token):
		BaseClient.__init__(self, token)
	def subscribe(self, object_scope, push_topic_id, user_id):
		path = '%s/topics/%s/push/subscriptions/users/%s' % (object_scope, push_topic_id, bucket, user_id)
		self._send(path, 'PUT', {})
	def unsubscribe(self, object_scope, push_topic_id, user_id):
		path = '%s/topics/%s/push/subscriptions/users/%s' % (object_scope, push_topic_id, bucket, user_id)
		self._send(path, 'DELETE', {})
	def create(self, object_scope, push_topic_id):
		path = '%s/topics/%s' % (object_scope,  push_topic_id)
		data = q.to_map()
		self._send(path, 'PUT', {})
	def delete(self, object_scope, push_topic_id):
		path = '%s/topics/%s' % (object_scope,  push_topic_id)
		self._send(path, 'DELETE', {})