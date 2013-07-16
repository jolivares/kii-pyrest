import httplib2
import json
import time
from kiicommon import BaseClient
from kiiuser import UserClient

class ObjectScope(object):
	def __init__(self, app, user, group):
		self.app = app
		self.user = user
		self.group = group
	@classmethod
	def for_app(cls, app):
		return cls(app, None, None)
	@classmethod
	def for_user(cls, app, user):
		return cls(app, user, None)
	@classmethod
	def for_group(cls, app, group):
		return cls(app, None, group)
	def __repr__(self):
		path = '/apps/%s' % (self.app)
		if self.user != None:
			path = '%s/users/%s' % (path, self.user)
		elif self.group != None:
			path = '%s/groups/%s' % (path, self.group)
		return path

class Query(object):
	def __init__(self, clause):
		self.clause = clause
	@classmethod
	def with_clause(cls, clause):
		return cls({'clause': clause.q})
	def order_by(self, field, descending = False):
		self.field = field
		self.descending = descending
		return self
	def to_map(self):
		q = {'bucketQuery': self.clause}
		if hasattr(self, 'field') and self.field != None:
			q.get('bucketQuery')['orderBy'] = self.field
			q.get('bucketQuery')['descending'] = self.descending
		return q

class C(object):
	def __init__(self, q):
		self.q = q
	@classmethod
	def cAll(cls):
		return cls({'type': 'all'})
	@classmethod
	def cEq(cls, field, value):
		return cls({'type': 'eq', 'field': field, 'value': value})
	@classmethod
	def cRange(cls, field, upper_limit, lower_limit, upper_included, lower_included):
		return cls({'type': 'range', 'field': field, 'upperLimit': upper_limit, 'lowerLimit': lower_limit,
			'upperIncluded': upper_included, 'lowerIncluded': lower_limit})
	@classmethod
	def cIn(cls, field, values):
		return cls({'type': 'in', 'field': field, 'values': values})
	@classmethod
	def cWithInBox(cls, field, sw, ne):
		return cls({'type': 'geobox', 'field': field, 'box': {'sw': sw, 'ne': ne}})
	@classmethod
	def cWithInDistance(cls, field, center, radius, result_field = None):
		return cls({'type': 'geodistance', 'field': field, 'center': center, 
			'radius': radius, 'resultField': result_field })
	@classmethod
	def cAnd(cls, *clauses):
		return cls({'type': 'and', 'clauses': [c.q for c in clauses]})
	@classmethod
	def cOr(cls, *clauses):
		return cls({'type': 'or', 'clauses': [c.q for c in clauses]})

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
		headers = {'content-type': 'application/vnd.kii.QueryRequest+json'}
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
		path = '%s/buckets/%s:%s/acl/%s' % (object_scope, bucket, acl)
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
