from vk_api_wrapper import API
from typing import List, Dict

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


##########################################


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
