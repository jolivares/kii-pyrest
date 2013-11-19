import httplib2
import json
import time
from kiicommon import BaseClient
from query import Query

class ObjectScope(object):
	def __init__(self, scope_type, app, user, group):
		self.type = scope_type
		self.app = app
		self.user = user
		self.group = group
	@classmethod
	def for_app(cls, app):
		return cls('APP', app, None, None)
	@classmethod
	def for_user(cls, app, user):
		return cls('APP_AND_USER', app, user, None)
	@classmethod
	def for_group(cls, app, group):
		return cls('APP_AND_GROUP', app, None, group)
	def to_map(self):
		return {'type': self.type, 'appID': self.app, 
			'userID': self.user, 'groupID': self.group}
	def __repr__(self):
		path = '/apps/%s' % (self.app)
		if self.user != None:
			path = '%s/users/%s' % (path, self.user)
		elif self.group != None:
			path = '%s/groups/%s' % (path, self.group)
		return path

class ACL(object):
	def __init__(self, subject, verb):
		self.subject = subject
		self.verb = verb
	@classmethod
	def for_user(cls, user, verb):
		return cls('UserID:' + user, verb)
	@classmethod
	def for_group(cls, group, verb):
		return cls('GroupID:' + group, verb)
	def __repr__(self):
		return '%s/%s' % (self.verb, self.subject)

class ACLVerb(object):
	CREATE_NEW_BUCKET = 'CREATE_NEW_BUCKET'
	CREATE_NEW_TOPIC = 'CREATE_NEW_TOPIC'
	QUERY_OBJECTS_IN_BUCKET = 'QUERY_OBJECTS_IN_BUCKET'
	CREATE_OBJECTS_IN_BUCKET = 'CREATE_OBJECTS_IN_BUCKET'
	DROP_BUCKET_WITH_ALL_CONTENT = 'DROP_BUCKET_WITH_ALL_CONTENT'
	READ_EXISTING_OBJECT = 'READ_EXISTING_OBJECT'
	WRITE_EXISTING_OBJECT = 'WRITE_EXISTING_OBJECT'
	SUBSCRIBE_TO_TOPIC = 'SUBSCRIBE_TO_TOPIC'
	SEND_MESSAGE_TO_TOPIC = 'SEND_MESSAGE_TO_TOPIC'

class BucketQuery(Query):
	def to_map(self):
		return self._to_map('bucketQuery')

class BucketType(object):
	READ_WRITE = 'rw'
	SYNC = 'sync'

class DataObjectClient(BaseClient):	
	def __init__(self, token, bucket_type):
		BaseClient.__init__(self, token)
		self.bucket_type = bucket_type
	def create(self, object_scope, bucket, data_type, data):
		path = '%s/buckets/%s:%s/objects' % (object_scope,  self.bucket_type, bucket)
		headers = {'Content-Type' : data_type}
		res = self._send(path, 'POST', headers, data)
		return res['objectID']
	def get(self, object_scope, bucket, object_id):
		path = '%s/buckets/%s:%s/objects/%s' % (object_scope,  self.bucket_type, bucket, object_id)
		headers = {}
		return self._send(path, 'GET', headers)
	def query(self, object_scope, bucket, q):
		path = '%s/buckets/%s:%s/query' % (object_scope,  self.bucket_type, bucket)
		headers = {'Content-Type': 'application/vnd.kii.QueryRequest+json'}
		data = q.to_map()
		res = self._send(path, 'POST', headers, data)
		return res['results'], res['nextPaginationKey'] if 'nextPaginationKey' in res else None
	def patch(self, object_scope, bucket, object_id, data_type, data):
		path = '%s/buckets/%s:%s/objects/%s' % (object_scope,  self.bucket_type, bucket, object_id)
		headers = {'Content-Type' : data_type}
		res = self._send(path, 'PATCH', headers, data)
		return res
	def replace(self, object_scope, bucket, object_id, data_type, data):
		path = '%s/buckets/%s:%s/objects/%s' % (object_scope,  self.bucket_type, bucket, object_id)
		headers = {'Content-Type' : data_type}
		res = self._send(path, 'PUT', headers, data)
		return res
	def delete(self, object_scope, bucket, object_id):
		path = '%s/buckets/%s:%s/objects/%s' % (object_scope,  self.bucket_type, bucket, object_id)
		headers = {}
		self._send(path, 'DELETE', headers)
	def add_scope_acl(self, object_scope, acl):
		path = '%s/acl/%s' % (object_scope, acl)
		self._send(path, 'PUT', {})
	def add_bucket_acl(self, object_scope, bucket, acl):
		path = '%s/buckets/%s:%s/acl/%s' % (object_scope, self.bucket_type, bucket, acl)
		self._send(path, 'PUT', {})
	def add_object_acl(self, object_scope, bucket, object_id, acl):
		path = '%s/buckets/%s:%s/objects/%s/acl/%s' % (object_scope, self.bucket_type, bucket, object_id, acl)
		self._send(path, 'PUT', {})
	def check_body(self, object_scope, bucket, object_id):
		path = '%s/buckets/%s:%s/objects/%s/body' % (object_scope, self.bucket_type, bucket, object_id)
		self._send(path, 'HEAD', {})
	def get_body(self, object_scope, bucket, object_id, etag = None):
		path = '%s/buckets/%s:%s/objects/%s/body' % (object_scope, self.bucket_type, bucket, object_id)
		headers = {}
		if etag != None:
			headers['If-Match'] = etag
		return self._send(path, 'GET', headers, {})
	def update_body(self, object_scope, bucket, object_id, data_type, data):
		path = '%s/buckets/%s:%s/objects/%s/body' % (object_scope, self.bucket_type, bucket, object_id)
		headers = {'Content-Type': data_type }
		return self._send(path, 'PUT', headers, data)
	def get_body_chunk(self, object_scope, bucket, object_id, start, end, etag = None):
		path = '%s/buckets/%s:%s/objects/%s/body' % (object_scope, self.bucket_type, bucket, object_id)
		headers = {'Range': "bytes=%d-%d" % (start, end)}
		if etag != None:
			headers['If-Match'] = etag
		return self._send(path, 'GET', headers)
	def start_upload(self, object_scope, bucket, object_id):
		path = '%s/buckets/%s:%s/objects/%s/body/uploads' % (object_scope, self.bucket_type, bucket, object_id)
		headers = {'Content-Type': 'application/vnd.kii.StartObjectBodyUploadRequest+json',
			'Accept': 'application/vnd.kii.StartObjectBodyUploadResponse+json' }
		res = self._send(path, 'POST', headers, {})
		return res['uploadID']
	def upload_chunk(self, object_scope, bucket, object_id, data_type, upload_id, data, start, end, total):
		path = '%s/buckets/%s:%s/objects/%s/body/uploads/%s/data' % (object_scope, self.bucket_type, bucket, object_id, upload_id)
		headers = {'Content-Range': 'bytes=%d-%d/%d' % (start, end, total), 'Content-Type': data_type }
		res = self._send(path, 'PUT', headers, data)
	def commit_upload(self, object_scope, bucket, object_id, upload_id):
		path = '%s/buckets/%s:%s/objects/%s/body/uploads/%s/status/committed' % (object_scope, self.bucket_type, bucket, object_id, upload_id)
		res = self._send(path, 'POST', {}, {})
	def move_body(self, source_object_scope, source_bucket, source_object_id, target_object_scope, target_bucket, target_object_id):
		path = '%s/buckets/%s:%s/objects/%s/body/move' % (source_object_scope, self.bucket_type, source_bucket, source_object_id)
		headers = {'Content-Type': 'application/vnd.kii.ObjectBodyMoveRequest+json'}
		req = {'targetObjectScope': target_object_scope.to_map(), 
			'targetBucketID': target_bucket, 'targetObjectID': target_object_id}
		res = self._send(path, 'POST', headers, req)
