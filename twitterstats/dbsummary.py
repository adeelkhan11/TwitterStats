import logging
import sqlite3
from dataclasses import dataclass, field
from typing import List

from twitterstats.dbutil import DBUtil
import oauth2 as oauth

from twitterstats.secommon import now

logger = logging.getLogger(__name__)


@dataclass
class PublishTweetItem:
    rank: int
    subrank: str
    score: str
    tweet_text: str
    display_image: str
    display_text: str

    def publish_dict(self):
        return {'rank': self.rank, 'subrank': self.subrank, 'score': self.score,
                'tweet_text': self.tweet_text, 'display_image': self.display_image,
                'display_text': self.display_text}


class DBSummary(DBUtil):
    def __init__(self, environment):
        DBUtil.__init__(self, environment)

        database = self.se_database('se_summary')
        try:
            self.conn = sqlite3.connect(database)
        except sqlite3.OperationalError as e:
            print('Unable to connect to database {}:'.format(database), e)
            raise
        self.c = self.conn.cursor()
        self.tokens = []
        self.curr_token_index = -1
        self.default_token_index = -1
        self.polling_token_index = -1
        self.load_tokens()

    def not_retweeted(self, retweet_id):
        t = ('PakPolStats', retweet_id)
        self.c.execute('select tweet_id from retweets where account = ? and tweet_id = ?', t)
        t = self.c.fetchone()
        if t is None:
            return 1
        else:
            return 0

    # Tokens
    def load_tokens(self):
        self.c.execute('SELECT screen_name, token, token_secret from twitter_account')

        rows = self.c.fetchall()

        for sn, t, ts in rows:
            self.tokens.append({'name': sn, 'token': oauth.Token(key=t, secret=ts)})
            if sn == self.env.default_account:
                self.default_token_index = len(self.tokens) - 1
            if sn == self.env.polling_account:
                self.polling_token_index = len(self.tokens) - 1

    def get_next_token(self):
        self.curr_token_index += 1
        if self.curr_token_index >= len(self.tokens):
            self.curr_token_index = 0

        return self.tokens[self.curr_token_index]['token']

    def get_default_token(self):
        return self.tokens[self.default_token_index]['token']

    @property
    def polling_token(self):
        return self.tokens[self.polling_token_index]['token']

    def get_all_tokens(self):
        _tokens = []

        for t in self.tokens:
            _tokens.append(t['token'])

        return _tokens

    def get_pending_tweets(self):
        results = list()
        t = ('pend-post', 'pend-rej', 'retweet')
        self.c.execute(
            'SELECT t_id, type, head, tail, image_head, date_nkey, period, status, tweet_id, account, background_image'
            ' from tweet where status IN (?, ?) and type in (?) order by tweet_id LIMIT 3',
            t)
        rows = self.c.fetchall()
        logger.info('%d retweets', len(rows))
        results.extend(rows)
        t = ('pend-post', 'pend-rej', 'trends', 'mentions', 'trenders')
        self.c.execute(
            'SELECT t_id, type, head, tail, image_head, date_nkey, period, status, tweet_id, account, background_image'
            ' from tweet where status IN (?, ?) and type in (?, ?, ?) order by drafted_at LIMIT 10',
            t)
        rows = self.c.fetchall()
        logger.info('%d stats', len(rows))
        results.extend(rows)

        tweets = list()
        for row in results:
            tweet = PublishTweet(self, *row)
            # tweet = {'t_id': row[0], 'type': row[1], 'head': row[2], 'tail': row[3],
            #          'image_head': row[4], 'date_nkey': row[5], 'period': row[6],
            #          'status': row[7], 'tweet_id': row[8], 'account': row[9],
            #          'background_image': row[10]};
            # tweet['items'] = list()

            if tweet.type != 'retweet':
                t = (tweet.id, 'Y')
                self.c.execute(
                    'SELECT rank, subrank, score, tweet_text, display_image, display_text'
                    ' from tweet_item where t_id = ? and selected = ? order by rank, subrank',
                    t)
                rows = self.c.fetchall()
                for row2 in rows:
                    item = PublishTweetItem(*row2)
                    tweet.items.append(item)
                # while row2 is not None:
                #     item = {'rank': row2[0], 'subrank': row2[1], 'score': row2[2], 'tweet_text': row2[3],
                #             'display_image': row2[4], 'display_text': row2[5]}
                #     tweet['items'].append(item)
                #     row2 = self.c.fetchone()
            tweets.append(tweet)

        return tweets

    def save_tweet_posted_status(self, _id, status):
        t = (status, now(), _id)
        self.c.execute('UPDATE tweet set status = ?, posted_at = ? where t_id = ?', t)
        self.commit()

    def save_retweet(self, tweet_id):
        t = (tweet_id, self.env.default_account, now())
        self.c.execute('insert into retweets (tweet_id, account, retweeted_at) values (?, ?, ?)', t)
        self.commit()


@dataclass
class PublishTweet:
    db_summary: DBSummary
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
    items: List[PublishTweetItem] = field(default_factory=list)

    def save_status(self):
        self.db_summary.save_tweet_posted_status(self.id, self.status)

    def publish_dict(self):
        return {'t_id': self.id, 'type': self.type, 'head': self.head, 'tail': self.tail,
                'image_head': self.image_head, 'date_nkey': self.date_nkey, 'period': self.period,
                'status': self.status, 'tweet_id': self.tweet_id, 'account': self.account,
                'background_image': self.background_image,
                'items': [item.publish_dict() for item in self.items]}
