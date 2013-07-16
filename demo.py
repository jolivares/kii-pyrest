import httplib2

from kii_pyrest.kiicommon import BaseClient, init_kii, auth_app
from kii_pyrest.kiiuser import UserClient
from kii_pyrest.kiiobject import DataObjectClient, ACL, ACLVerb, ObjectScope, Query, C

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

init_kii(APP, APP_KEY, API_ENDPOINT)
ADMIN_TOKEN = auth_app(APP_CLIENT_ID, APP_CLIENT_SECRET)
# kii clients
object_client = DataObjectClient(ADMIN_TOKEN, 'rw')
user_client = UserClient(ADMIN_TOKEN)

def user_management():
	# user creation
	rq = {'password': 'xxxx', 'loginName': 'demo_user'}
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
	q = Query.with_clause(C.cAnd(
	 	C.cEq('thread_id', 3434), 
	 	C.cEq('read', True),
	)).order_by('title', False)
	
	messages, paginationKey = object_client.query(scope, bucket, q)
	
	# mark read messages as unread
	for message in messages:
		unread = {'read': False}
		object_client.patch(scope, bucket, message['_id'], object_type, unread)

	# remove all messages
  	q = Query.with_clause(C.cAll())	
	messages, paginationKey = object_client.query(scope, bucket, q)

	for message in messages:
		object_client.delete(scope, bucket, message['_id'])

def acl_management():
	# TODO 
	pass

if __name__ == '__main__':
	httplib2.debuglevel = 1

	user_management()
	object_management()
	acl_management()

