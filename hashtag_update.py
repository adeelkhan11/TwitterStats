import logging

from datetime import timedelta
import datetime

import defaults
from twitterstats.db import DB
from twitterstats.secommon import today

logger = logging.getLogger('hashtag_update')


def main():
    environment = defaults.get_environment()
    db = DB(environment, today())

    start_date = str(datetime.date.today() - timedelta(days=7))
    end_date = str(datetime.date.today() - timedelta(days=1))
    logger.info(f'Dates: {start_date} {end_date}')
    words = {}
    rows = db.get_top_hashtags(start_date, end_date)
    for row in rows:
        words[row[0].lower()] = row[0]

    logger.info(f'{len(words)} words')

    for word, hashtag in words.items():
        db.set_word_hashtag(word, hashtag)
        logger.debug(f'{word:>30} {hashtag}')

    db.disconnect()


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
