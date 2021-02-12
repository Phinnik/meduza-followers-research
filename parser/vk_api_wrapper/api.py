import requests
import time
from typing import List

__all__ = ['API']


class API:
    def __init__(self, access_token: str):
        self._access_token = access_token
        self._session = requests.Session()

    def _call(self, method_name: str, params: dict = None):
        params = params or dict()
        api_url = f'https://api.vk.com/method/{method_name}'
        params = {k: v for k, v in params.items() if v is not None}
        params.pop('self', None)
        params['v'] = '5.126'
        params['access_token'] = self._access_token
        request_time_start = time.time()
        response = self._session.post(api_url, data=params).json()
        delay = 1 / 2.5 - (time.time() - request_time_start)
        if delay > 0:
            time.sleep(delay)
        if 'error' in response:
            raise Exception(response['error'])
        else:
            return response['response']

    def execute(self, code: str):
        params = locals()
        return self._call('execute', params)

    def groups_get_members(self, group_id: int):
        params = locals()
        return self._call('groups.getMembers', params)

    def users_get(self, user_ids: List[int], fields: List[str] = None):
        params = {
            'user_ids': str(user_ids)[1:-1].replace(' ', ''),
            'fields': None if fields is None else ','.join(fields)
        }
        print(params)
        return self._call('users.get', params)

    def messages_send(self, user_id: int, message: str):
        params = locals()
        params['random_id'] = time.time()
        return self._call('messages.send', params)
