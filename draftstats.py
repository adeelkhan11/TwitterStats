# This version uses backup pic if error obtaining profile pic
# import secommon
from dataclasses import dataclass, field
from typing import List

import defaults
# import env
import operator
import math
import os
import datetime
import os.path
# import DBUtil
import time
import argparse

from twitterstats.db import DB
# from twitterstats.dbsummary import PublishTweet
from twitterstats.dbsummary import DBSummary
from twitterstats.publisher import Publisher
from twitterstats.secommon import now
from twitterstats.urdu import urdu_to_english
import logging

logger = logging.getLogger('draftstats')
DAILY_STAT_TWEET_LIMIT = 7

# logger = logging.getLogger('simpleExample')


@dataclass
class DraftStatsTweetSubItem:
    score: int
    tweet_text: str
    star_count: int
    # a: int
    # b: int
    # c: int
    display_image: str = None
    display_text: str = None
    category: str = None
    subrank: int = None
    tweet_count: int = None
    tweep_count: int = None
    rt_count: int = None
    rt_received_count: int = None
    botness: int = None

    def publish_dict(self):
        result = {'score': self.score, 'tweet_text': self.tweet_text,
                  'a': int(self.star_count / 25),
                  'b': int((self.star_count % 25) / 5),
                  'c': int(self.star_count % 5)}
        if self.display_image is not None:
            result['display_image'] = self.display_image
        if self.display_text is not None:
            result['display_text'] = self.display_text
        if self.category is not None:
            result['category'] = self.category
        if self.tweet_count is not None:
            result['tweet_count'] = int(self.tweet_count)
        if self.tweep_count is not None:
            result['tweep_count'] = int(self.tweep_count)
        if self.rt_count is not None:
            result['rt_count'] = int(self.rt_count)
        if self.rt_received_count is not None:
            result['rt_received_count'] = int(self.rt_received_count)
        if self.botness is not None:
            result['botness'] = self.botness
        return result


@dataclass
class DraftStatsTweetItem:
    rank: int
    subitems: List[DraftStatsTweetSubItem] = field(default_factory=list)

    def publish_dict(self):
        result = {'rank': self.rank,
                  'subitems': [si.publish_dict() for si in self.subitems]}
        return result


@dataclass
class DraftStatsTweet:
    id: str
    type: str
    head: str
    tail: str
    image_head: str
    date_nkey: str
    period: str
    status: str
    tweet_id: str
    account: str
    background_image: str
    tweet_screen_name: str = None
    drafted_at: str = None
    trend: str = None
    items: List[DraftStatsTweetItem] = field(default_factory=list)

    def publish_dict(self):
        result = {'type': self.type, 'head': self.head, 'tail': self.tail,
                  'image_head': self.image_head, 'date_nkey': self.date_nkey, 'period': self.period,
                  'account': self.account,
                  'items': [item.publish_dict() for item in self.items]}
        if self.status is not None:
            result['status'] = self.status
        if self.tweet_id is not None:
            result['tweet_id'] = self.tweet_id
        if self.background_image is not None:
            result['background_image'] = self.background_image
        if self.tweet_screen_name is not None:
            result['tweet_screen_name'] = self.drafted_at
        if self.drafted_at is not None:
            result['drafted_at'] = self.drafted_at
        if self.trend is not None:
            result['trend'] = self.trend
        return result


class Stats:
    def __init__(self, calcdate, type, db, actions, environment, trend=None):
        self.date = calcdate
        self.type = type
        self.db = db
        self.trend = trend
        self.hashtags = dict()
        self.tag_history = dict()
        self.items = list()
        self.tweeps = dict()
        self.env = environment

        logger.info("Drafting [%s] %s %s", self.date, self.type, '' if self.trend is None else self.trend)

        if type == 'trends':
            self.get_trend_groups()
        elif type == 'trenders':
            self.get_tweeters_for_trend()
        elif type == 'mentions':
            self.get_mentions()

        sorted_x = sorted(self.items, key=operator.itemgetter(1), reverse=True)
        i = 0
        # new_dict = {}
        sorted_y = []
        for x, y in sorted_x:
            if i >= 20 and ((type == 'trends' and y < 250) or (type == 'trenders' and y < 100) or y < 50):
                break
            if type != 'trenders':
                logger.debug("%5d: %s", y, ' '.join([a for a, b in x]))
            sorted_y.append([x, y])
            subindex = 0
            for a, b in x:
                subindex += 1
                if type == 'trenders':
                    logger.debug("%d: %s %d", y, a, b)
                # else:
                # logger.info(a)
                if type == 'trends' and subindex <= 3 and (
                        (i < 11 and subindex == 1) or (i <= 20 and a.lower()[1:] in self.tag_history and y >= 700)):
                    completeness = db.get_tag_completeness(a)
                    print("Completeness for %s: %0.3f" % (a, completeness))
                    if completeness > 0.999:
                        actions.append({'type': 'trenders', 'trend': a[1:]})
            # if type != 'trenders':
            # print
            i += 1

        self.items = sorted_y

    def get_tweeter_details(self, screen_name):
        t = (screen_name,)
        self.db.c.execute('SELECT profile_image_url, name, category FROM dim_tweeter WHERE screen_name=?', t)
        row = self.db.c.fetchone()
        profile_image_url = None
        name = None
        category = None
        if row is not None:
            (profile_image_url, name, category) = row

        return profile_image_url, name, category

    # @staticmethod
    # def month_delta(date, delta):
    #     m, y = (date.month + delta) % 12, date.year + (date.month + delta - 1) // 12
    #     if not m: m = 12
    #     d = min(date.day, [31,
    #                        29 if y % 4 == 0 and not y % 400 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][
    #         m - 1])
    #     return date.replace(day=d, month=m, year=y)

    def get_tweeters_for_trend(self):
        # trend = actions[action_ind]['trend']
        english_trend = urdu_to_english(self.trend)
        tweeps = self.db.get_retweet_received_counts_for_trend(self.date, english_trend)

        tweeters = []
        for item, scores in tweeps.items():
            rt_received_count = 0
            botness = 0
            if 'rt_received_count' in scores:
                rt_received_count = scores['rt_received_count']
                botness = scores['botness']
            rt_count = 0
            if 'rt_count' in scores:
                rt_count = scores['rt_count']
            tweet_count = 0
            if 'tweet_count' in scores:
                tweet_count = scores['tweet_count']
            score = (rt_received_count + botness) + rt_count + tweet_count
            tweeters.append([[[item, score]], score])
        self.items = tweeters
        self.tweeps = tweeps

    def get_mentions(self):
        tweeters = {}
        rows = self.db.get_top_mentions_for_date(self.date)
        for i, row in enumerate(rows):
            if i > 20 or int(row[1]) < 50:
                break
            tweeters[row[0]] = int(row[1])

        items = []
        for item, score in tweeters.items():
            items.append([[[item, score]], score])
        self.items = items

    def get_trend_groups(self):

        # Get list of hashtags from tag history - we can't be certain of tweet
        # and tweep counts for tags not in this list
        self.tag_history = self.db.get_tags_from_tag_history(self.date)
        logger.debug("tag_history length: %d", len(self.tag_history))
        date_skey = self.db.get_date_skey(self.date)

        groups = []
        rows = self.db.get_trend_groups(date_skey)
        logger.debug('%d rows', len(rows))
        for (word1, word2) in rows:
            found = 0
            for group in groups:
                if word1 in group or word2 in group:
                    if word1 not in group:
                        group[word1] = 0
                    if word2 not in group:
                        group[word2] = 0
                    found = 1
                    break
            if not found:
                g = {word1: 0, word2: 0}
                groups.append(g)

        rows = self.db.get_tag_botness(date_skey)

        tweepcounts = dict()
        for word, tweeps, botness in rows:
            word = urdu_to_english(word)
            if word in tweepcounts:
                tweepcounts[word] += (tweeps + botness) / 2
            else:
                tweepcounts[word] = (tweeps + botness) / 2
            if tweepcounts[word] < 1:
                tweepcounts[word] = 1

        rows = self.db.get_tag_tweet_counts(date_skey)

        self.hashtags = {'pakistan': {'tag': 'Pakistan', 'tweets': 0, 'tweeps': 0}}
        for row in rows:
            word = urdu_to_english(row[0].lower())
            if word in self.hashtags:
                self.hashtags[word]['tag'] = row[0]
                self.hashtags[word]['tweets'] += row[1]
            else:
                self.hashtags[word] = {'tag': row[0], 'tweets': row[1], 'tweeps': 0}
            if word in tweepcounts:
                self.hashtags[word]['tweeps'] = tweepcounts[word]
            found = 0
            for group in groups:
                if word in group:
                    group[word] += row[1]
                    found = 1
                    break
            if not found:
                g = {word: row[1]}
                groups.append(g)

        # adjust scores based on tweep counts
        for group in groups:
            for trend, score in group.items():
                if trend in tweepcounts:
                    group[trend] = int(math.floor(tweepcounts[trend] * math.sqrt(score / tweepcounts[trend])))
                else:
                    group[trend] = 0

        logger.debug("%d groups", len(groups))

        newgroups = []

        # sort the groups (only take the first 3 trends of each group)
        for group in groups:
            sorted_x = sorted(group.items(), key=operator.itemgetter(1), reverse=True)

            sorted_y = list()
            for a, b in sorted_x:
                if a in self.hashtags:
                    na = '#' + self.hashtags[a]['tag']
                else:
                    na = '#' + a
                sorted_y.append([na, b])

            if sorted_y[0][1] > 100:
                newgroups.append([sorted_y, sorted_y[0][1]])

        self.items = newgroups

    def write_tweet(self, count):
        period = 'day'  # Only worried about daily stats for now
        showdate = datetime.datetime.strptime(self.date, '%Y-%m-%d').strftime('%d %B %Y').lstrip('0')
        trend = self.trend if self.trend is not None else ''

        tweet = DraftStatsTweet(
            id='{}-{}'.format(self.date, self.trend if self.type == 'trenders' else self.type),
            type=self.type,
            head={'mentions': "Top mentions " + showdate + ":\n",
                  'trends': "Trends " + showdate + ":\n",
                  'trenders': "Top tweeps for #%s:\n" % trend}[self.type],
            tail={'mentions': "\n-\n#PTI #PMLN #PPP",
                  'trends': "\n-\n#PTI #PMLN #PPP",
                  'trenders': ""}[self.type],
            image_head={'mentions': "Top mentions for the " + period,
                        'trends': "Top trends for the " + period,
                        'trenders': "Top tweeps for #%s" % trend}[self.type],
            drafted_at=now(),
            date_nkey=self.date,
            tweet_screen_name=self.env.default_account,
            account=self.env.default_account,
            period=period,
            trend=self.trend,
            background_image=None,
            status='pend-post',
            tweet_id=None
        )
        i = 0
        while i < len(self.items) and i < count:
            item = DraftStatsTweetItem(rank=i + 1)
            if self.type != 'trends':
                screen_name = self.items[i][0][0][0]
                # logger.info(f'Screen name: {screen_name}')
                score = self.items[i][1]
                starcount = int(score / 25) if self.type == 'mentions' else int(score / 100)
                # else:
                #     if screen_name in self.tweeps:
                #         subitem.update(self.tweeps[screen_name])
                (url, name, category) = self.get_tweeter_details(screen_name)

                # item['rank'] = i + 1
                # item['subitems'] = list()
                subitem = DraftStatsTweetSubItem(score=score,
                                                 star_count=starcount,
                                                 display_image=url,
                                                 tweet_text='@' + screen_name,
                                                 display_text=name,
                                                 category=category)
                if self.type == 'trenders' and screen_name in self.tweeps:
                    tweep = self.tweeps[screen_name]
                    subitem.tweet_count = tweep.get('tweet_count')
                    subitem.rt_count = tweep.get('rt_count')
                    subitem.rt_received_count = tweep.get('rt_received_count')
                    subitem.botness = tweep.get('botness')
                # subitem['a'] = int(starcount / 25)
                # subitem['b'] = int((starcount % 25) / 5)
                # subitem['c'] = int(starcount % 5)
                # subitem['display_image'] = url
                # subitem['tweet_text'] = '@' + screen_name
                # subitem['display_text'] = name
                # subitem['category'] = category
                item.subitems.append(subitem)
                logger.debug("%3d %5d %s", item.rank, subitem.score, subitem.tweet_text)
            else:
                # item = dict()
                # item['rank'] = i + 1
                # item['subitems'] = list()
                subrank = 1
                for t, s in self.items[i][0]:
                    subitem = DraftStatsTweetSubItem(score=s,
                                                     tweet_text=t,
                                                     subrank=subrank,
                                                     star_count=int(s / 250))
                    # subitem['score'] = s
                    # subitem['tweet_text'] = t
                    # subitem['subrank'] = subrank
                    # starcount = int(subitem['score'] / 250)
                    # subitem['a'] = int(starcount / 25)
                    # subitem['b'] = int((starcount % 25) / 5)
                    # subitem['c'] = int(starcount % 5)
                    word = t.lower()[1:]
                    english_word = urdu_to_english(word)
                    if english_word in self.hashtags and word in self.tag_history:
                        subitem.tweet_count = self.hashtags[english_word]['tweets']
                        subitem.tweep_count = self.hashtags[english_word]['tweeps']

                    subrank += 1
                    item.subitems.append(subitem)

            tweet.items.append(item)
            i += 1
        return tweet

    @staticmethod
    def is_trenders_tweet_postable(tweet):
        result = True
        foreign_accounts = list()
        top_3_bots = list()
        all_bots = list()
        rejection_reasons = list()
        last_score = 0
        for i, item in enumerate(tweet.items):
            if i < 20:
                last_score = item.subitems[0].score
            if item.subitems[0].category == 'F':
                foreign_accounts.append(item.subitems[0].display_text)
            elif item.subitems[0].category == 'R':
                all_bots.append(item.subitems[0].display_text)
                if i < 3:
                    top_3_bots.append(item.subitems[0].display_text)

        if len(foreign_accounts) > 1:
            result = False
            rejection_reasons.append('More than 1 foreign account ({})'.format(', '.join(foreign_accounts)))

        if len(top_3_bots) > 1:
            result = False
            rejection_reasons.append('More than 1 bot in top 3 places ({})'.format(', '.join(top_3_bots)))

        if len(all_bots) > 7:
            result = False
            rejection_reasons.append('More than 7 bots in top 20 places ({})'.format(', '.join(all_bots)))

        if len(tweet.items) < 20:
            result = False
            rejection_reasons.append(f'{len(tweet.items)} items is less than 20')

        if last_score < 50:
            result = False
            rejection_reasons.append(f'Last item score of {last_score} is less than 50')

        if not result:
            logger.info('Rejected stats for {}: [{}]'.format(tweet.trend, '; '.join(rejection_reasons)))

        return result


def main():
    parser = argparse.ArgumentParser(description='Draft stats for the given day and push to cloud for approval.')
    parser.add_argument('date', metavar='yyyy-mm-dd',
                        help='the date to process')

    args = parser.parse_args()

    environment = defaults.get_environment()
    db = DB(environment, args.date)
    db_summary = DBSummary(environment)

    date_skey = db.get_date_skey(args.date)

    actions = list()
    action = {'type': 'trends'}
    actions.append(action)
    action = {'type': 'mentions'}
    actions.append(action)

    action_ind = 0
    tweets = list()
    stat_tweet_count = 0
    while action_ind < len(actions):
        action = actions[action_ind]['type']
        # tweeters = None
        stats = Stats(args.date, action, db, actions, environment,
                      actions[action_ind]['trend'] if action == 'trenders' else None)

        i = 100
        is_tweetable = True

        if action == "trenders":
            tweet = stats.write_tweet(i)
            if not stats.is_trenders_tweet_postable(tweet) or stat_tweet_count >= DAILY_STAT_TWEET_LIMIT:
                is_tweetable = False
        elif action == "trends":
            tweet = stats.write_tweet(i)
        elif action == "mentions":
            tweet = stats.write_tweet(i)

        if is_tweetable:
            db_summary.save_tweet(tweet)
            stat_tweet_count += 1

        if tweet is not None:
            tweets.append(tweet)
        if len(tweets) >= 2:
            data = {'tweets': tweets, 'date': args.date}
            Publisher.publish(environment, data, 'draft')
            tweets = list()
            time.sleep(10)
        action_ind += 1

    db_summary.disconnect()

    # Now get app metrics
    rows = db.get_tweeter_category_counts()

    metric_dict = {'date': args.date, 'other': 0}
    for cat, count in rows:
        if cat is None:
            cat = ' '
        if cat in ('A', 'B', 'C', 'D', 'E', 'F', 'R', ' '):
            metric_dict[cat] = count
        else:
            metric_dict['other'] += count

    # Get count of total tweets and tweets by category
    rows = db.get_tweeter_category_tweet_counts(date_skey)

    metric_dict['tweets_total'] = 0
    metric_dict['tweets_other'] = 0
    for cat, count in rows:
        metric_dict['tweets_total'] += count

        if cat is None:
            cat = ' '
        if cat in ('A', 'B', 'C', 'D', 'E', 'F', 'R', ' '):
            metric_dict['tweets_' + cat] = count
        else:
            metric_dict['tweets_other'] += count

    # Add file sizes
    metric_dict['fact_db_size'] = os.path.getsize(environment.database)
    metric_dict['dim_db_size'] = os.path.getsize(environment.dimension_database)
    metric_dict['summ_db_size'] = os.path.getsize(environment.summary_database)

    followers_count = db.get_tweeter_followers_count('pakpolstats')
    metric_dict['account_followers'] = followers_count

    data = {'tweets': tweets, 'metrics': metric_dict, 'date': args.date}
    Publisher.publish(environment, data, 'draft')


# jdata = { 'tweets' : tweets, 'date': args.date }
# jtext = json.dumps(jdata)
# print len(jtext), jtext
#
#
# url = env.base_url + 'deliver'
# values = {'data' : jtext,
#          'type' : 'draft' }
#
# data = urllib.urlencode(values)
# #data = data.encode('utf-8') # data should be bytes
# req = urllib2.Request(url, data)
# response = urllib2.urlopen(req)
# the_page = response.read()
#
# print "--"
# print the_page

# db_disconnect()


if __name__ == '__main__':
    import logging.config
    import yaml

    # import TlsSMTPHandler

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
