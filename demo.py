import httplib2
import tempfile

from kii_pyrest.kiicommon import BaseClient, init_kii, auth_app
from kii_pyrest.kiiuser import UserClient, UserQuery
from kii_pyrest.kiiobject import DataObjectClient, ACL, ACLVerb, ObjectScope, BucketType, BucketQuery
from kii_pyrest.query import C
from kii_pyrest.kiiservercode import ServerCodeClient

try:
    import credentials
except ImportError:
    sys.exit('You must create a credentials.py file with API_ENDPOINT, ' \
                 'APP, APP_KEY, APP_CLIENT_ID, APP_CLIENT_SECRET variables set')

API_ENDPOINT = getattr(credentials, 'API_ENDPOINT')
APP = getattr(credentials, 'APP')
APP_KEY = getattr(credentials, 'APP_KEY')
APP_CLIENT_ID = getattr(credentials, 'APP_CLIENT_ID')
APP_CLIENT_SECRET = getattr(credentials, 'APP_CLIENT_SECRET')

# init kii credentials
init_kii(APP, APP_KEY, API_ENDPOINT)
ADMIN_TOKEN = auth_app(APP_CLIENT_ID, APP_CLIENT_SECRET)
# init kii clients
object_client = DataObjectClient(ADMIN_TOKEN, BucketType.READ_WRITE)
user_client = UserClient(ADMIN_TOKEN)
servercode_client = ServerCodeClient(ADMIN_TOKEN)

def user_management():
	# user creation
	rq = {'password': 'xxxx', 'loginName': 'demo_user2'}
	user = user_client.create(rq)
	userID = user['userID']
	# user retrieval by id
	user = user_client.get(userID)	
	# remove the user
	user_client.remove(userID)

def object_management():
	# scope = app - objects will belong to the app scope in this case, so they will be available to every user
	scope = ObjectScope.for_app(APP)
	# bucket / folder in which objects will be stored
	bucket = "messages"
	# type for our objects
	object_type = "application/json"

	data1 = {'title': 'msg1', 'thread_id': 3434, 'read': True}
	data2 = {'title': 'msg2', 'thread_id': 3434, 'read': True}
	data3 = {'title': 'msg3', 'thread_id': 3435, 'read': False}
	
	object_id1 = object_client.create(scope, bucket, object_type, data1)
	object_id2 = object_client.create(scope, bucket, object_type, data2)
	object_id3 = object_client.create(scope, bucket, object_type, data3)

	# get read messages by thread
	q = BucketQuery.with_clause(C.cAnd(
	 	C.cEq('thread_id', 3434), 
	 	C.cEq('read', True),
	)).order_by('title', False)
	
	messages, paginationKey = object_client.query(scope, bucket, q)

	# mark read messages as unread
	for message in messages:
		unread = {'read': False}
		object_client.patch(scope, bucket, message['_id'], object_type, unread)

	# remove all messages
  	q = BucketQuery.with_clause(C.cAll())	
	messages, paginationKey = object_client.query(scope, bucket, q)

	for message in messages:
		object_client.delete(scope, bucket, message['_id'])

def body_management():
	# scope = app - objects will belong to the app scope in this case, so they will be available to every user
	scope = ObjectScope.for_app(APP)
	# bucket / folder in which objects will be stored
	bucket = "messages"
	# type for our objects
	object_type = "application/json"

	data = {'title': 'msg1', 'thread_id': 3434, 'read': True}
	
	object_id = object_client.create(scope, bucket, object_type, data)
	# start upload
	upload_id = object_client.start_upload(scope, bucket, object_id)
	
	data_type = 'application/javascript'
	data_file = 'function sayHello(args) {return "Hello " + args.name + "!"}'	
	# range stuff
	length = len(data_file)
	range_size = length // 4
	start = 0
	end = range_size - 1
	# uploading
	while start < length - 1:
		chunk = data_file[start:end + 1]
		object_client.upload_chunk(scope, bucket, object_id, data_type, upload_id, 
			chunk, start, end, length)
		
		start = end + 1
		next = end + range_size
		end = next if next < length else length - 1
	# commit upload
	object_client.commit_upload(scope, bucket, object_id, upload_id)

def acl_management():
	# TODO 
	pass

def server_code_management():
	code = 'function sayHello(args) {return "Hello " + args.name + "!"}'
	f = tempfile.NamedTemporaryFile()
	f.write(code)
	f.flush()

	version_id = servercode_client.deploy(f.name)

	versions = servercode_client.list()
	assert len([x for x in versions if x['versionID'] == version_id]) == 1

	assert code == servercode_client.get(version_id)
	
	endpoint = "sayHello"	

	res = servercode_client.invoke("current", endpoint, {'name': 'Coder'})
	assert res['returnedValue'] == 'Hello Coder!'

	servercode_client.delete(version_id)

if __name__ == '__main__':
	httplib2.debuglevel = 1

	# body_management()
	# user_management()
	# object_management()
	# acl_management()
	server_code_management()

