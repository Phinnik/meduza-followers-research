import logging
from vk_api_wrapper.logging_message_handler import MessageHandler
from vk_parser import Parser, UserData
import pandas as pd
import json
import pathlib
from typing import Dict, List
from tqdm import tqdm

# read config
with open('config.json') as fc:
    config = json.load(fc)

# logging stuff
######################################################################################################################
logger = logging.getLogger('parser')
logger.setLevel(logging.DEBUG)

# formatters
formatter = logging.Formatter('{asctime} - {levelname} - {message}', style='{')
vk_formatter = logging.Formatter('{levelname} - {message}', style='{')

# handlers
if config['logging']['console']['enabled']:
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

if config['logging']['file']['enabled']:
    file_handler = logging.FileHandler(config['logging']['file']['file_path'])
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

if config['logging']['vk_message']['enabled']:
    message_handler = MessageHandler(config['logging']['vk_message']['vk_access_token'],
                                     config['logging']['vk_message']['user_log_id'])
    message_handler.setLevel(logging.WARNING)
    message_handler.setFormatter(vk_formatter)
    logger.addHandler(message_handler)


######################################################################################################################


class Main:
    def __init__(self):
        self.parser = Parser(config['vk_access_token'])
        self.create_data_folder()
        self.data_fp = self.get_data_fp()
        self.log_df = self.get_log_df()
        self.user_data_df = self.get_user_data_df()
        self.friends_connections_df = self.get_friends_connections_df()
        self.user_groups_df = self.get_user_groups_df()

    @staticmethod
    def get_data_fp():
        data_fp = {
            'log': f'{config["data_fp"]}/users.log.csv',
            'user_data': f'{config["data_fp"]}/user_data.csv',
            'friends_connections': f'{config["data_fp"]}/friends_connections.csv',
            'user_groups': f'{config["data_fp"]}/user_groups.json'
        }
        return data_fp

    @staticmethod
    def create_data_folder():
        """
        Creates data folder specified in config
        """
        pathlib.Path(f'{config["data_fp"]}').mkdir(parents=True, exist_ok=True)

    def get_log_df(self) -> pd.DataFrame:
        """
        Reads or creates log data frame and returns it
        """
        if pathlib.Path(self.data_fp['log']).exists():
            log_df = pd.read_csv(self.data_fp['log'], index_col='user_id')
        else:
            log_df = pd.DataFrame(columns=['user_id', 'is_closed', 'data_parsed',
                                           'friends_parsed', 'groups_parsed']).set_index('user_id')
            log_df.to_csv(self.data_fp['log'])
        return log_df

    def get_user_data_df(self) -> pd.DataFrame:
        """
        Reads or creates user data frame and returns it
        """
        if pathlib.Path(self.data_fp['user_data']).exists():
            user_data_df = pd.read_csv(self.data_fp['user_data'], index_col='user_id')
        else:
            user_data_df = pd.DataFrame(columns=list(UserData.__annotations__.keys())).set_index('user_id')
            user_data_df.to_csv(self.data_fp['user_data'])

        return user_data_df

    def get_friends_connections_df(self) -> pd.DataFrame:
        """
        Reads or creates friends connections data frame and returns it
        """
        if pathlib.Path(self.data_fp['friends_connections']).exists():
            friends_connections_df = pd.read_csv(self.data_fp['friends_connections'], index_col='user_a')
        else:
            friends_connections_df = pd.DataFrame(columns=['user_a', 'user_b']).set_index('user_a')
            friends_connections_df.to_csv(self.data_fp['friends_connections'])
        return friends_connections_df

    def get_user_groups_df(self) -> Dict[int, List[int]]:
        """
        Reads or creates user groups dict and returns it
        """
        if pathlib.Path(self.data_fp['user_groups']).exists():
            with open(self.data_fp['user_groups'], 'r', encoding='utf-8') as f:
                user_groups_df = json.load(f)
        else:
            user_groups_df = dict()
            with open(self.data_fp['user_groups'], 'w', encoding='utf-8') as f:
                json.dump(user_groups_df, f, ensure_ascii=False, indent=2)
        return user_groups_df

    def parse_members_ids(self):
        """
        Collects group member ids and puts it into data frame
        """
        if len(self.log_df) == 0:
            logger.debug('Start parsing members ids')
            ids = self.parser.get_members_ids(config['group_id'])
            new_log = pd.DataFrame({
                'user_id': ids,
                'is_closed': [None] * len(ids),
                'data_parsed': [None] * len(ids),
                'friends_parsed': [None] * len(ids),
                'groups_parsed': [None] * len(ids)
            }).set_index('user_id')
            self.log_df = self.log_df.append(new_log)
            self.log_df.to_csv(self.data_fp['log'])
            logger.debug('Finish parsing members ids')
            logger.info(f'Got {len(ids)} member ids from group https://vk.com/public{config["group_id"]}')

    def parse_user_data(self):
        """
        Collects user data and puts it into data frame
        """
        unparsed_ids = self.log_df[self.log_df['data_parsed'].isna()].index.to_list()
        if len(unparsed_ids) > 0:
            logger.debug('Start parsing user data')
            ids_packs = [unparsed_ids[i:i + 5000] for i in range(0, len(unparsed_ids), 5000)]

            tqdm_pbar = tqdm(ids_packs, desc='collecting user data', ncols=80)
            for pack in tqdm_pbar:
                data = self.parser.get_users_data(pack)
                new_users = pd.DataFrame([d.__dict__ for d in data]).set_index('user_id')
                self.user_data_df = self.user_data_df.append(new_users)
                self.user_data_df.to_csv(self.data_fp['user_data'])
                self.log_df.loc[pack, 'data_parsed'] = True
                self.log_df.loc[pack, 'is_closed'] = [d.is_closed for d in data]
                self.log_df.to_csv(self.data_fp['log'])
                logger.debug(f'Collected data about {len(data)} users')
            logger.debug('Finish parsing user data')

    def parse_friends_connections(self):
        """
        Collects friends connections and puts it into data frame
        """
        user_ids = set(self.log_df.index.to_list())
        unparsed_ids = self.log_df[(self.log_df['friends_parsed'].isna()) & (self.log_df['is_closed'] == False)]
        unparsed_ids = unparsed_ids.index.to_list()
        if len(unparsed_ids) > 0:
            logger.debug('Start parsing friends connections')
            ids_packs = [unparsed_ids[i: i + 250] for i in range(0, len(unparsed_ids), 250)]

            tqdm_pbar = tqdm(ids_packs, desc='collecting friends connections', ncols=80)
            for pack in tqdm_pbar:
                data = self.parser.get_users_friends(pack)
                if list(data.values()).count(None) == len(data):
                    logger.warning('Friend.get limit exceeded')
                    break
                logger.debug(f'Collected friends of {len(data)} users')
                friends_count = [len(friends) if friends else None for friends in data.values()]
                self.user_data_df.loc[pack, 'friends_count'] = friends_count
                for user_id, friends in data.items():
                    if friends is not None:
                        friends_connections = user_ids.intersection(set(friends))
                        friends_connections = pd.DataFrame({'user_a': [user_id] * len(friends_connections),
                                                            'user_b': list(friends_connections)}).set_index('user_a')
                        self.friends_connections_df = self.friends_connections_df.append(friends_connections)
                self.log_df.loc[pack, 'friends_parsed'] = True
                self.user_data_df.to_csv(self.data_fp['user_data'])
                self.friends_connections_df.to_csv(self.data_fp['friends_connections'])
                self.log_df.to_csv(self.data_fp['log'])
            logger.debug('Finish parsing friends connections')

    def parse_user_groups(self):
        """
        Collects user groups and puts it into data frame
        """
        unparsed_ids = self.log_df[(self.log_df['groups_parsed'].isna()) & (self.log_df['is_closed'] == False)]
        unparsed_ids = unparsed_ids.index.to_list()
        ids_packs = [unparsed_ids[i: i + 250] for i in range(0, len(unparsed_ids), 250)]
        if len(unparsed_ids) > 0:
            logger.debug('Start parsing user groups')
            tqdm_pbar = tqdm(ids_packs, desc='collecting users groups', ncols=80)
            for pack in tqdm_pbar:
                data = self.parser.get_users_groups(pack)
                if list(data.values()).count(None) == len(data):
                    logger.warning('Groups.get limit exceeded')
                    break
                logger.debug(f'Got groups of {len(data)} users')
                self.user_groups_df.update(data)
                with open(self.data_fp['user_groups'], 'w', encoding='utf-8') as f:
                    json.dump(self.user_groups_df, f, ensure_ascii=False, indent=2)
            logger.debug('Finish parsing user groups')

    def run(self):
        """
        Runs parsing cycle
        """
        self.parse_members_ids()
        self.parse_user_data()
        self.parse_friends_connections()
        self.parse_user_groups()


if __name__ == '__main__':
    try:
        main = Main()
        main.run()
    except Exception as e:
        logger.error(e)
        raise e
