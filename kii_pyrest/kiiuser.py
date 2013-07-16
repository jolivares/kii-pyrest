import httplib2
import json
from kiicommon import BaseClient

class UserClient(BaseClient):
	def __init__(self, token):
		BaseClient.__init__(self, token)
	def create(self, user_data):
		path = '/apps/%s/users' % (self._get_app())
		headers = { 'Content-Type' : 'application/vnd.kii.RegistrationRequest+json',
			'Accept' : 'application/vnd.kii.RegistrationResponse+json' }
		res = self._send(path, 'POST', headers, user_data)
		return res
	def get(self, user_id):
		path = '/apps/%s/users/%s' % (self._get_app(), user_id)
		headers = {'Accept' : 'application/vnd.kii.UserDataRetrievalResponse+json'}
		return self._send(path, 'GET', headers)
	def get_by_login_name(self, login_name):
		path = '/apps/%s/users/LOGIN_NAME:%s' % (self._get_app(), login_name)
		headers = {'Accept' : 'application/vnd.kii.UserDataRetrievalResponse+json'}
		return self._send(path, 'GET', headers)
	def remove(self, user_id):
		path = '/apps/%s/users/%s' % (self._get_app(), user_id)
		headers = {}
		self._send(path, 'DELETE', headers, {})
	def get_status(self):
		path = '/apps/%s/users/%s/status' % (self._get_app(), user_id)
		headers = {'Accept' : 'application/vnd.kii.UserStatusRetrievalResponse+json'}
		res = self._send(path, 'GET', headers)
		return res['disabled']
	def set_status(self, user_id, disabled):
		path = '/apps/%s/users/%s/status' % (self._get_app(), user_id)
		headers = { 'Content-Type' : 'application/vnd.kii.UserStatusUpdateRequest+json'}
		return self._send(path, 'PUT', headers, {'disabled' : disabled })
	def change_password(self, user_id, old_pass, new_pass):
		path = '/apps/%s/users/%s/password' % (self._get_app(), user_id)
		headers = { 'Content-Type' : 'application/vnd.kii.ChangePasswordRequest+json' }
		rq = {'oldPassword': old_pass, 'newPassword': new_pass}
		self._send(path, 'PUT', headers, rq)
	def create_group(self, name, owner, members):
		path = '/apps/%s/groups' % (self._get_app())
		headers = {'Content-Type' : 'application/vnd.kii.GroupCreationRequest+json'}
		rq = {'owner': owner, 'members': members, 'name': name}
		return self._send(path, 'POST', headers, rq)
	def get_group(self, group_id):
		path = '/apps/%s/groups/%s' % (self._get_app(), group_id)
		headers = {}
		return self._send(path, 'GET', headers, rq)
	def find_groups_by_owner(self, owner):
		path = '/apps/%s/groups?owner=%s' % (self._get_app(), owner)
		return self._send(path, 'GET', {})
	def find_groups_by_member(self, member):
		path = '/apps/%s/groups?is_member=%s' % (self._get_app(), member)
		return self._send(path, 'GET', {})
	def add_member_to_group(self, group_id, user_id):
		path = '/apps/%s/groups/%s/members/%s' % (self._get_app(), group_id, user_id)
		return self._send(path, 'PUT', {})


 