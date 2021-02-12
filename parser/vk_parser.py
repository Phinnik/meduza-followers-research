from vk_api_wrapper import API
from typing import List, Dict
from datetime import datetime
from dataclasses import dataclass

# VK scripts
##########################################


get_user_ids_script = """
var offset = {offset};
var group_id = {group_id};
var i = 0;
var user_ids = [];
while (i < 25) {{
    var new_user_ids = API.groups.getMembers({{
        "group_id": group_id, 
        "offset": offset + i * 1000, 
        "count": 1000}})["items"];
    user_ids = user_ids + new_user_ids; 
    i = i + 1;
}}
return user_ids;
"""

get_friends_script = """
var user_ids = {user_ids};
var i = 0;
var users_friends = [];
while ((i < 25) && (i < user_ids.length)) {{
    var friends = API.friends.get({{
        "user_id": user_ids[i],
        "count": 1000
    }});
    users_friends = users_friends + [friends];
    i = i + 1;
}}
return users_friends;
"""

get_many_friends_script = """
var user_id = {user_id};
var user_friends = API.friends.get({{
    "user_id": user_id,
    "count": 1000
}});
var friends_count = user_friends["count"];
user_friends = user_friends["items"];
var offset = 1000;
while (friends_count - offset > 0) {{
    var friends = API.friends.get({{
        "user_id": user_id,
        "offset": offset,
        "count": 1000
    }})["items"];
    user_friends = user_friends + friends;
    offset = offset + 1000;
}}
return user_friends;
"""

get_groups_script = """
var user_ids = {user_ids};
var user_groups = [];
var i = 0;
while ((i < 25) && (i < user_ids.length)) {{
    var groups = API.groups.get({{
        "user_id": user_ids[i], 
        "count": 30
    }})["items"];
    user_groups = user_groups + [groups];
    i = i + 1;
}}
return user_groups;
"""

get_users_data_script = """
var ids_packs = {};
var users_data = [];
var i = 0;
while ((i < 2) && (i < ids_packs.length)) {{
    var data = API.users.get({{
        "user_ids": ids_packs[i]
    }});
    users_data = users_data + data;
    i = i + 1;
}}
return users_data;
"""


##########################################

@dataclass
class UserData:
    user_id: int
    is_closed: bool
    sex: int
    birth_date: datetime
    verified: bool
    city: str
    country: str
    university_name: str
    faculty_name: str
    last_seen: datetime
    can_write_private_message: bool
    can_send_friend_request: bool
    followers_count: int
    friends_count: int

    @classmethod
    def from_users_get(cls, data):
        user_id = data.get('id', None)
        is_closed = data.get('is_closed', None)
        sex = data.get('sex', None)

        # sometimes birth date has bugs in vk
        birth_date = data.get('bdate', '')
        try:
            if birth_date.count('.') == 2:
                birth_date = datetime.strptime(birth_date, '%d.%m.%Y')
            elif birth_date.count('.') == 1:
                birth_date = datetime.strptime(birth_date, '%d.%m')
            else:
                birth_date = None
        except ValueError:
            birth_date = None

        verified = data.get('verified', None)
        city = data.get('city', dict()).get('title', None)
        country = data.get('country', dict()).get('title')
        university_name = data.get('university_name', None)
        faculty_name = data.get('faculty_name', None)
        last_seen = data.get('last_seen', dict()).get('time', 0)
        last_seen = datetime.fromtimestamp(last_seen)
        can_write_private_message = bool(data.get('can_write_private_message', None))
        can_send_friend_request = bool(data.get('can_send_friend_request', None))
        followers_count = data.get('followers_count', None)
        friends_count = data.get('friends_count', None)
        return cls(user_id, is_closed, sex, birth_date, verified, city, country, university_name, faculty_name,
                   last_seen, can_write_private_message, can_send_friend_request, followers_count, friends_count)


class Parser:
    def __init__(self, access_token: str):
        self.api = API(access_token)

    def get_group_members_count(self, group_id: int) -> int:
        members_count = self.api.groups_get_members(group_id)['count']
        return members_count

    def get_members_ids(self, group_id: int) -> List[int]:
        members_count = self.get_group_members_count(group_id)
        user_ids = []
        for offset in range(0, members_count, 25000):
            new_user_ids = self.api.execute(get_user_ids_script.format(group_id=group_id, offset=offset))
            user_ids.extend(new_user_ids)
        return user_ids

    def get_many_friends(self, user_id: int) -> List[int]:
        friends = self.api.execute(get_many_friends_script.format(user_id=user_id))
        return friends

    def get_users_friends(self, user_ids: List[int]) -> Dict[int, List[int]]:
        ids_pack = [user_ids[i: i + 25] for i in range(0, len(user_ids), 25)]
        user_friends = dict()
        for pack in ids_pack:
            users_friends = self.api.execute(get_friends_script.format(user_ids=str(pack)))
            for user_id, friends in zip(pack, users_friends):
                if friends is False:
                    user_friends[user_id] = None
                elif friends['count'] < 1000:
                    user_friends[user_id] = friends['items']
                else:
                    user_friends[user_id] = self.get_many_friends(user_id)
        return user_friends

    def get_users_groups(self, user_ids: List[int]) -> Dict[int, List[int]]:
        ids_pack = [user_ids[i: i + 25] for i in range(0, len(user_ids), 25)]
        user_groups = dict()
        for pack in ids_pack:
            users_groups = self.api.execute(get_groups_script.format(user_ids=str(pack)))
            for user_id, groups in zip(pack, users_groups):
                user_groups[user_id] = groups
        return user_groups

    def get_users_data(self, user_ids: List[int]) -> List[UserData]:
        user_packs = [user_ids[i:i + 1000] for i in range(0, len(user_ids), 1000)]
        users_data = []
        for pack in user_packs:
            data = self.api.users_get(pack, fields=['sex', 'bdate', 'verified', 'city', 'country', 'education',
                                                    'last_seen', 'followers_count', 'can_write_private_message',
                                                    'can_send_friend_request'])
            users_data.extend([UserData.from_users_get(d) for d in data])
        return users_data
