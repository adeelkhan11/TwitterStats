import logging

import defaults
from twitterstats.db import DB
from twitterstats.dbsummary import DBSummary
from twitterstats.secommon import today
from twitterstats.twitterapi import TwitterAPI


class TwitterList:
    def __init__(self, environment, db, db_summary):
        self.environment = environment
        self.db = db
        self.db_summary = db_summary
        self.api = TwitterAPI(environment, db_summary)

    def add_to_lists(self):
        additions = self.db.get_list_additions()
        self.db.load_list_levels()
        for screen_name, category, original_list in additions:
            if original_list is not None:
                self.remove_from_list(screen_name, original_list)
            list_name = self.get_smallest_list(category)
            self.add_to_list(screen_name, list_name)

    def add_to_list(self, screen_name, list_name):
        error = self.api.add_to_list(screen_name, list_name)
        _list_name = list_name if error is None else f'ERROR: {error}'
        self.db.add_to_list(screen_name, _list_name)
        self.db.commit()
        logger.info(f'{_list_name}: {screen_name} added to list')

    def remove_from_lists(self):
        removals = self.db.get_list_removals()
        for screen_name, category, original_list in removals:
            self.remove_from_list(screen_name, original_list)
            self.db.add_to_list(screen_name, list_name=None)

    def remove_from_list(self, screen_name, list_name):
        self.api.remove_from_list(screen_name, list_name)
        self.db.remove_from_list(screen_name)
        self.db.commit()
        logger.info(f'{list_name}: {screen_name} removed from list')

    def get_smallest_list(self, category):
        smallest_count = None
        for l in self.environment.lists[category]:
            if smallest_count is None or l['count'] < smallest_count:
                smallest_list = l['name']
                smallest_count = l['count']
        # Now increment count of this list so next time another list can be picked
        for l in self.environment.lists[category]:
            if l['name'] == smallest_list:
                l['count'] += 1
        return smallest_list


logger = logging.getLogger('findbots_behaviour')


def main():
    environment = defaults.get_environment()
    db = DB(environment, today())
    db_summary = DBSummary(environment)

    tl = TwitterList(environment, db, db_summary)

    tl.add_to_lists()
    tl.remove_from_lists()

    db.disconnect()


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
