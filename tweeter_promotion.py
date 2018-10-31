# Does not promote tweeps already categorised from F to Z
import logging

from datetime import date, timedelta, datetime as time
import datetime
# from secommon import *

import defaults
from twitterstats.db import DB
from twitterstats.secommon import today

PROMOTION_THRESHOLD = 10
POWER_TWEEP = 10000
logger = logging.getLogger('tweeter_promotion')


def main():
    env = defaults.get_environment()
    db = DB(env, today())

    demotedate_c = (datetime.date.today() - timedelta(days=30)).strftime('%Y-%m-%d')
    demotedate_d = (datetime.date.today() - timedelta(days=90)).strftime('%Y-%m-%d')

    # Promote to C
    logger.info("Tweeter Promotion  %s" % today())

    rows = db.get_tweeter_promotion_stats()

    # If a person has more than POWER_TWEEP followers, then mark it as F if it is negative - cannot do it for all
    # tweeps because
    # would get too many category F's. Don't want to waste resources storing Tweeps we may never encounter in future.
    for screen_name, pos, neg, blocked, category, relevance_score, followers_count in rows:
        if relevance_score is None:
            relevance_score = 0
        new_relevance_score = relevance_score
        if blocked > 3 and blocked > pos and relevance_score <= -10:
            action = 'B'
        elif neg > pos and (category is not None or relevance_score != 0 or followers_count >= POWER_TWEEP):
            action = '-'
            if neg > 3:
                new_relevance_score -= 2
            else:
                new_relevance_score -= 1
        else:  # pos >= neg
            action = '+'
            if pos > 3:
                new_relevance_score += 2
            elif pos > 1:
                new_relevance_score += 1

        newcat = category
        if new_relevance_score >= PROMOTION_THRESHOLD:
            action = '++'
            if category == 'D':
                newcat = 'C'
            elif category == 'E' or category is None:
                newcat = 'D'
            elif category == 'F':
                newcat = None
        elif new_relevance_score <= -PROMOTION_THRESHOLD:
            action = '--'
            if category == 'C':
                newcat = 'D'
            elif category in ('D', 'E') or followers_count >= POWER_TWEEP:
                newcat = 'F'

        if new_relevance_score > PROMOTION_THRESHOLD:
            new_relevance_score = PROMOTION_THRESHOLD
        if new_relevance_score < -PROMOTION_THRESHOLD:
            new_relevance_score = -PROMOTION_THRESHOLD
        if newcat != category or new_relevance_score != relevance_score:
            if newcat != category:
                new_relevance_score = 0
            logger.info("%4s %-2s %-4s %4d %4d %s" % (
                category, action, newcat, relevance_score, new_relevance_score, screen_name))
            db.set_tweeter_category(screen_name=screen_name,
                                    category=newcat,
                                    relevance_score=new_relevance_score)

    # Demote from D
    db.set_tweeter_category_by_date(date_category_was_set=demotedate_d,
                                    current_category='D',
                                    new_category='E')

    # Demote from C
    db.set_tweeter_category_by_date(date_category_was_set=demotedate_c,
                                    current_category='C',
                                    new_category='D')

    db.disconnect()


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
