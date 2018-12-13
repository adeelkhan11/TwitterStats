# Does not promote tweeps already categorised from F to Z
import logging
from dataclasses import dataclass

from datetime import timedelta
import datetime
# from secommon import *

import defaults
from twitterstats.db import DB
from twitterstats.secommon import today, rank_words

PROMOTION_THRESHOLD = 10
POWER_TWEEP = 10000
logger = logging.getLogger('tweeter_promotion')


@dataclass
class Tweeter:
    screen_name: str
    name: str = None
    category: str = None
    relevance_score: int = None
    location: str = None
    time_zone: str = None
    followers_count: int = None
    score_adjustment: int = 0
    new_category: str = None

    def __post_init__(self):
        if self.category is None:
            self.name, self.category, self.relevance_score, self.location, self.time_zone, self.followers_count = \
                DB.db.get_tweeter_relevance_score(self.screen_name)
        if self.relevance_score is None:
            self.relevance_score = 0
        self.new_category = self.category
        self.promote_score()

    def adjust_score(self, adjustment):
        self.score_adjustment += adjustment

    def save_score(self):
        self.relevance_score += self.score_adjustment
        action = ''
        if self.score_adjustment > 0:
            action = '+'
        elif self.score_adjustment < 0:
            action = '-'
        if self.relevance_score >= PROMOTION_THRESHOLD:
            action = '++'
            if self.category == 'D':
                self.new_category = 'C'
            elif self.category == 'E' or self.category is None:
                self.new_category = 'D'
            elif self.category == 'F':
                self.new_category = None
        elif self.relevance_score <= -PROMOTION_THRESHOLD:
            action = '--'
            if self.category == 'C':
                self.new_category = 'D'
            elif self.category in ('D', 'E') or (self.category is None and self.followers_count >= POWER_TWEEP):
                self.new_category = 'F'

        if self.relevance_score > PROMOTION_THRESHOLD:
            self.relevance_score = PROMOTION_THRESHOLD
        if self.relevance_score < -PROMOTION_THRESHOLD:
            self.relevance_score = -PROMOTION_THRESHOLD
        if self.new_category != self.category or self.score_adjustment != 0:
            if self.new_category != self.category:
                self.relevance_score = 0
            logger.info("%4s %-2s %-4s %4d %4d %s" % (
                self.category, action, self.new_category, self.score_adjustment,
                self.relevance_score, self.screen_name))
            DB.db.set_tweeter_category(screen_name=self.screen_name,
                                       category=self.new_category,
                                       relevance_score=self.relevance_score)

    def promote_score(self):
        name_score = DB.db.get_name_score(self.name, self.screen_name, self.location, self.time_zone,
                                          ignore_category=True)
        if name_score >= 10:
            self.score_adjustment += 3
        elif name_score >= 5:
            self.score_adjustment += 1
        elif name_score <= -3:
            self.score_adjustment -= 1
        elif name_score <= -5:
            self.score_adjustment -= 3


class Promotion:
    def __init__(self):
        self.tweeters = dict()

    def add(self, screen_name, name=None, category=None, relevance_score=None, location=None, time_zone=None,
            followers_count=None):
        if screen_name not in self.tweeters:
            self.tweeters[screen_name] = Tweeter(screen_name=screen_name,
                                                 name=name,
                                                 category=category,
                                                 relevance_score=relevance_score,
                                                 location=location,
                                                 time_zone=time_zone,
                                                 followers_count=followers_count)
        return self.tweeters[screen_name]

    def save_all(self):
        for tweeter in self.tweeters.values():
            tweeter.save_score()


def main():
    env = defaults.get_environment()
    db = DB(env, today())
    promotion = Promotion()

    demotedate_c = (datetime.date.today() - timedelta(days=30)).strftime('%Y-%m-%d')
    demotedate_d = (datetime.date.today() - timedelta(days=90)).strftime('%Y-%m-%d')

    # Promote to C
    logger.info("Tweeter Promotion  %s" % today())

    rows = db.get_tweeter_promotion_stats()

    # If a person has more than POWER_TWEEP followers, then mark it as F if it is negative - cannot do it for all
    # tweeps because
    # would get too many category F's. Don't want to waste resources storing Tweeps we may never encounter in future.
    for screen_name, pos, neg, blocked, category, relevance_score, followers_count, name, location, time_zone in rows:
        tweeter = promotion.add(screen_name=screen_name,
                                name=name,
                                category=category,
                                relevance_score=relevance_score,
                                location=location,
                                time_zone=time_zone,
                                followers_count=followers_count)
        # if relevance_score is None:
        #     relevance_score = 0
        # adjustment = 0
        if blocked > 3 and blocked > pos and relevance_score <= -10:
            tweeter.new_category = 'B'
        elif neg > pos and (category is not None or relevance_score != 0 or followers_count >= POWER_TWEEP):
            if neg > 3:
                tweeter.adjust_score(-2)
            else:
                tweeter.adjust_score(-1)
        else:  # pos >= neg
            if pos > 3:
                tweeter.adjust_score(2)
            elif pos > 1:
                tweeter.adjust_score(1)

    # Promote top tweeps
    db.c.execute('select screen_name from dim_tweeter where category <= ?', ('C',))
    rows = db.c.fetchall()
    famous = [row[0] for row in rows]

    trenders = rank_words(f'{env.bot_data_directory}/trenders_published_%s.txt', 7)
    non_famous = [trender for trender in trenders if trender not in famous]
    for screen_name in non_famous[:50]:
        tweeter = promotion.add(screen_name=screen_name)
        tweeter.adjust_score(1)

    promotion.save_all()

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
