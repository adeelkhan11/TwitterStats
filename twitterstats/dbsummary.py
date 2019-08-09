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
    subrank: int
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

    def not_selected_for_retweet(self, retweet_id):
        t = (self.env.default_account, retweet_id, 'posted', 'pend-post')
        self.c.execute('select tweet_id from tweet where account = ? and tweet_id = ? and status in (?, ?)', t)
        t = self.c.fetchone()
        if t is None:
            return 1
        else:
            return 0

    def not_retweeted(self, retweet_id):
        t = (self.env.default_account, retweet_id)
        self.c.execute('select tweet_id from retweets where account = ? and tweet_id = ?', t)
        t = self.c.fetchone()
        if t is None:
            return 1
        else:
            return 0

    def get_selected_for_retweet_since_id(self, since_id, since_date):
        t = (self.env.default_account, since_id, since_date, 'retweet', 'posted', 'pend-post')
        self.c.execute("""select tweet_id from tweet where account = ? and tweet_id >= ? and tweet_created_at >= ?
        and type = ? and status in (?, ?)""", t)
        rows = self.c.fetchall()
        return [row[0] for row in rows]

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

    def get_account_screen_names(self):
        return [t['name'] for t in self.tokens]

    def get_processed_commands(self, since_id):
        sql = """SELECT id FROM commands WHERE id > ?"""
        self.c.execute(sql, (since_id, ))
        result = self.c.fetchall()
        return frozenset([r[0] for r in result])

    def get_tweet_status(self, t_id):
        t = (t_id,)
        self.c.execute('SELECT status FROM tweet WHERE t_id = ?', t)
        row = self.c.fetchone()
        return row[0] if row is not None else None

    def get_pending_tweets(self):
        results = list()
        t = ('pend-post', 'pend-rej', 'pend-unpost', 'retweet')
        self.c.execute(
            'SELECT t_id, type, head, tail, image_head, date_nkey, period, status, tweet_id, account'
            ', background_image, retweet_id'
            ' from tweet where status IN (?, ?, ?) and type in (?) order by tweet_id LIMIT 3',
            t)
        rows = self.c.fetchall()
        logger.info('%d retweets', len(rows))
        results.extend(rows)
        t = ('pend-post', 'pend-rej', 'trends', 'mentions', 'trenders')
        self.c.execute(
            'SELECT t_id, type, head, tail, image_head, date_nkey, period, status, tweet_id, account'
            ', background_image, retweet_id'
            ' from tweet where status IN (?, ?) and type in (?, ?, ?) order by drafted_at LIMIT 10',
            t)
        rows = self.c.fetchall()
        logger.info('%d stats', len(rows))
        results.extend(rows)

        tweets = list()
        for row in results:
            t_id, ttype, head, tail, image_head, date_nkey, period, status, tweet_id, account, background_image, retweet_id = row
            tweet = PublishTweetWritable(type=ttype,
                                         head=head,
                                         tail=tail,
                                         image_head=image_head,
                                         date_nkey=date_nkey,
                                         period=period,
                                         status=status,
                                         tweet_id=tweet_id,
                                         account=account,
                                         background_image=background_image,
                                         id=t_id,
                                         retweet_id=retweet_id,
                                         db_summary=self)
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
        t = (status, now(), f'{_id}-{self.env.default_account}', self.env.default_account)
        self.c.execute('UPDATE tweet set status = ?, posted_at = ? where t_id = ? and account = ?', t)
        self.commit()

    def save_retweet(self, tweet_id):
        t = (tweet_id, self.env.default_account, now())
        self.c.execute('insert into retweets (tweet_id, account, retweeted_at) values (?, ?, ?)', t)
        self.commit()

    def save_tweet_retweet_id(self, tweet_id, retweet_id):
        t = (retweet_id, f'{tweet_id}-{self.env.default_account}')
        self.c.execute('update tweet set retweet_id = ? where t_id = ?', t)
        self.commit()

    def save_tweet_retweet(self, tweet, account, drafted_date):
        status = 'pend-post'
        score = f'{tweet.rank}: {tweet.score:.0f}/{tweet.bot_data_availability}'
        t = (f'{tweet.id}-{account}', 'retweet', tweet.id, tweet.text, score,
             tweet.screen_name, tweet.retweet_count, status,
             tweet.created_at, drafted_date, account,
             tweet.category)
        self.c.execute('INSERT INTO tweet (t_id, type, tweet_id, head, tail, tweet_screen_name, ' +
                       'tweet_retweet_count, status, tweet_created_at, drafted_at, ' +
                       'account, tweeter_type) ' +
                       'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', t)

    def delete_tweet_if_exists(self, tweet):
        self.c.execute('DELETE FROM tweet WHERE t_id = ?', (f'{tweet.id}-{tweet.account}', ))
        self.c.execute('DELETE FROM tweet_item WHERE t_id = ?', (f'{tweet.id}-{tweet.account}', ))

    def save_command(self, command):
        sql = "INSERT INTO commands (id, screen_name, created_at, text, processed_date, status) " \
              "VALUES (?, ?, ?, ?, ?, ?)"
        t = (command.id,
             command.screen_name,
             command.created_at,
             command.text,
             command.processed_date,
             command.status)

        self.c.execute(sql, t)

    def save_tweet(self, tweet):
        self.delete_tweet_if_exists(tweet)

        t = (f'{tweet.id}-{tweet.account}', tweet.type, tweet.tweet_id, tweet.head, tweet.tail,
             tweet.tweet_screen_name, None, tweet.status,
             None, tweet.drafted_at, now(),
             tweet.image_head, tweet.date_nkey, tweet.period, tweet.account,
             None, tweet.trend, tweet.background_image)
        self.c.execute('INSERT INTO tweet (t_id, type, tweet_id, head, tail, tweet_screen_name, ' +
                       'tweet_retweet_count, status, tweet_created_at, drafted_at, submitted_at, ' +
                       'image_head, date_nkey, period, account, tweeter_type, trend, background_image) ' +
                       'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', t)

        for item in tweet.items:
            self.save_tweet_item(tweet, item)

    def save_tweet_item(self, tweet, item):
        for sub_item in item.subitems:
            t = (
                f'{tweet.id}-{tweet.account}', item.rank, sub_item.subrank, sub_item.score, sub_item.tweet_text,
                sub_item.display_image,
                sub_item.display_text, 'Y')
            self.c.execute(
                """INSERT INTO tweet_item (t_id, rank, subrank, score, tweet_text, display_image, display_text, selected)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                t)


@dataclass
class PublishTweet:
    type: str
    head: str
    tail: str
    image_head: str
    date_nkey: str
    period: str
    status: str
    tweet_id: str
    retweet_id: str
    account: str
    background_image: str
    tweet_screen_name: str = None
    drafted_at: str = None
    trend: str = None
    items: List[PublishTweetItem] = field(default_factory=list)

    def publish_dict(self):
        result = {'type': self.type, 'head': self.head, 'tail': self.tail,
                  'image_head': self.image_head, 'date_nkey': self.date_nkey, 'period': self.period,
                  'account': self.account,
                  'items': [item.publish_dict() for item in self.items]}
        if self.status is not None:
            result['status'] = self.status
        if self.tweet_id is not None:
            result['tweet_id'] = self.tweet_id
        if self.retweet_id is not None:
            result['retweet_id'] = self.retweet_id
        if self.background_image is not None:
            result['background_image'] = self.background_image
        if self.tweet_screen_name is not None:
            result['tweet_screen_name'] = self.drafted_at
        if self.drafted_at is not None:
            result['drafted_at'] = self.drafted_at
        if self.trend is not None:
            result['trend'] = self.trend
        return result


@dataclass
class PublishTweetWritable(PublishTweet):
    id: str = None
    db_summary: DBSummary = None

    def save_status(self):
        self.db_summary.save_tweet_posted_status(self.id, self.status)

    def publish_dict(self):
        result = {'t_id': self.id, 'type': self.type, 'head': self.head, 'tail': self.tail,
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
