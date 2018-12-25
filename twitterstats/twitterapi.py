import logging
from time import sleep

import twitter
from twitter import TwitterError

logger = logging.getLogger(__name__)


class TwitterAPI:
    def __init__(self, environment, db_summary):
        self.environment = environment
        self.db_summary = db_summary

        self.twitters = list()
        self.load_api()

    def load_api(self):
        self.twitters = list()
        for token in self.db_summary.get_all_tokens():
            api = twitter.Api(consumer_key=self.environment.consumer_key,
                              consumer_secret=self.environment.consumer_secret,
                              access_token_key=token.key,
                              access_token_secret=token.secret,
                              timeout=10)
            self.twitters.append(api)

    def polling_api(self):
        return self.twitters[self.db_summary.polling_token_index]

    def remove_from_list(self, screen_name, list_name):
        try:
            self.polling_api().DestroyListsMember(screen_name=screen_name,
                                                  slug=list_name.lower(),
                                                  owner_screen_name=self.environment.polling_account)
            return
        except TwitterError as e:
            if e.message[0]['code'] == 110:
                logger.warning(f'The user ({screen_name}) you are trying to remove from the list ({list_name}) is ' +
                               'not a member.')
            elif e.message[0]['code'] == 108:
                logger.warning(
                    f'The user ({screen_name}) you are trying to remove from the list ({list_name}) does ' +
                    'not exist.')
            else:
                raise

    def add_to_list(self, screen_name, list_name):
        for _ in range(2):
            try:
                self.polling_api().CreateListsMember(screen_name=screen_name,
                                                     slug=list_name.lower(),
                                                     owner_screen_name=self.environment.polling_account)
                return None
            except TwitterError as e:
                if e.message[0]['code'] == 34:
                    self.polling_api().CreateList(list_name)
                    logger.info(f'Created list {list_name}.')
                    # self.load_api()
                    sleep(5)
                if e.message[0]['code'] == 104:
                    logger.error(f'Rate limit reached for adding user {screen_name} to list {list_name}.')
                    return e.message[0]['message']
                if e.message[0]['code'] == 106:
                    logger.error(f'Not allowed to add user {screen_name} to list {list_name}.')
                    return e.message[0]['message']
                if e.message[0]['code'] == 108:
                    logger.warning(f'Could not find user {screen_name} to add to list {list_name}.')
                    return e.message[0]['message']
                else:
                    raise
        raise TwitterError()
