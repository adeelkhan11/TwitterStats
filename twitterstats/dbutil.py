"""
Created on 18 Sep 2018

@author: Adeel Khan
"""

import sqlite3
import datetime
import os
import logging
import oauth2 as oauth
from twitterstats import secommon
import re
from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Tweet:
    id: int
    created_at: str
    screen_name: str
    text: str
    retweet_count: int
    category: str


@dataclass
class PublishTweet(Tweet):
    score: float
    bot_data_availability: bool
    rank: int = None
    drafted_date: str = None
    account: str = None

    def publish_dict(self):
        return {'tweet_id': str(self.id), 'type': 'retweet', 'tweeter_type': self.category,
                'tweet_created_at': self.created_at,
                'tweet_retweet_count': self.retweet_count,
                'bot_score': '{}/{}'.format(self.score, self.bot_data_availability), 'rank': self.rank,
                'tweet_screen_name': self.screen_name, 'head': self.text, 'drafted_at': self.drafted_date,
                'account': self.account}


logger = logging.getLogger(__name__)


class DBUtil:
    """
    With specific functions to connect to TwitterStats databases.
    """

    # c = None
    # conn = None

    def __init__(self, environment):
        """
        Constructor
        """
        self.env = environment
        self.c = None
        self.conn = None

    @staticmethod
    def get_file_date(date):
        return date.replace('-', '_')

    @staticmethod
    def get_file_month(date):
        return date[:7].replace('-', '_')

    def se_database(self, db_type, date='2000-01-01'):
        filename = None
        if db_type in ['se', 'se_dimension']:
            filename = (self.env.database_old if db_type == 'se' else self.env.dimension_database_old).format(
                self.get_file_date(date) if db_type == 'se' else self.get_file_month(date))
            if not os.path.isfile(filename):
                filename = self.env.database if db_type == 'se' else self.env.dimension_database
        elif db_type == 'se_summary':
            filename = self.env.summary_database
        return filename

    def commit(self):
        self.conn.commit()

    def disconnect(self):
        self.conn.commit()
        self.conn.close()

    @staticmethod
    def null_to_empty(value):
        if value is None:
            return ''
        return str(value)

    @staticmethod
    def null_to_zero(value):
        if value is None:
            return 0.0
        if isinstance(value, str):
            if value.strip() == '':
                return 0.0
            else:
                return float(value.replace(',', ''))
        return value

    @classmethod
    def sum_nullables(cls, values):
        # print "Summing:", values
        total = 0
        for v in values:
            total += cls.null_to_zero(v)
        return total

    def check_exists(self, table, keys, values):
        where_clause = ' and '.join(["%s = ?" % k for k in keys])
        sql = "select count(*) from %s where %s" % (table, where_clause)
        self.c.execute(sql, tuple(values))
        result = self.c.fetchone()
        return result[0] > 0

    @staticmethod
    def get_dict_key(my_dict, keys):
        return '_'.join([str(my_dict[k]) for k in keys])

    @classmethod
    def convert_rows_to_dict(cls, rows, keys, columns):
        all_columns = keys + columns
        column_count = len(all_columns)
        rows_dict = dict()
        for row in rows:
            row_dict = dict()
            for i in range(column_count):
                row_dict[all_columns[i]] = row[i] if all_columns[i] not in keys else str(row[i])
            rows_dict[cls.get_dict_key(row_dict, keys)] = row_dict
            # print self.getDictKey(row_dict, keys), row_dict
        return rows_dict

    def pivot_dicts(self, data, row_keys, column_keys, value):
        result = dict()
        for row in data:
            row_key = self.get_dict_key(row, row_keys)
            col_key = self.get_dict_key(row, column_keys)

            if row_key not in result:
                result[row_key] = dict()

            result[row_key][col_key] = row[value]
        return result

    def load_dicts(self, table, keys, columns, filter_text="?=?", filter_values=(1, 1)):
        all_columns = keys + columns
        # print "All columns", all_columns, keys, columns
        sql = "select %s from %s where %s" % (', '.join(all_columns), table, filter_text)
        self.c.execute(sql, tuple(filter_values))
        rows = self.c.fetchall()

        return self.convert_rows_to_dict(rows, keys, columns)

    def load_dicts_from_query(self, keys, columns, sql, filter_values=None):
        if filter_values is None:
            self.c.execute(sql)
        else:
            self.c.execute(sql, tuple(filter_values))
        rows = self.c.fetchall()

        return self.convert_rows_to_dict(rows, keys, columns)

    def save_dict(self, table, keys, rec, update_only=False):
        columns = rec.keys()
        values = [rec[k] for k in columns]
        if keys is not None and self.check_exists(table, keys, [rec[k] for k in keys]):
            set_clause = ', '.join(["%s = ?" % k for k in columns if k not in keys])
            where_clause = ' and '.join(["%s = ?" % k for k in keys])
            sql = "update %s set %s where %s" % (table, set_clause, where_clause)
            # print sql
            update_values = [rec[k] for k in columns if k not in keys]
            update_values.extend([rec[k] for k in keys])
            # print update_values
            self.c.execute(sql, tuple(update_values))
        elif update_only:
            logger.warning("Warning: Value %s not found in table %s. Cannot update.", '-'.join([rec[k] for k in keys]),
                           table)
            return False
        else:
            sql = "insert into %s (%s) values (%s)" % (table, ', '.join(columns), ', '.join(['?'] * len(columns)))
            # print sql
            self.c.execute(sql, values)
        return True

    def get_date_skey(self, date):
        if date not in self.date_skey_cache:
            # mark_database_activity()
            t = (date,)
            self.c.execute('select date_skey from dim_date where date = ?', t)
            row = self.c.fetchone()
            if row is None:
                self.c.execute('insert into dim_date (date) values (?)', t)
                self.c.execute('select date_skey from dim_date where date = ?', t)
                row = self.c.fetchone()
            self.date_skey_cache[date] = row[0]

        return self.date_skey_cache[date]

    def tweet_is_duplicate(self, _id, created_at, screen_name, text, tweeter_skey, retweet_count, in_reply_to_status_id,
                           date_skey, retweet_id, retweet_created_at, retweet_screen_name, batch_id):
        result = 1
        if _id not in self.fact_status and _id not in self.fact_status_retweet_count:
            t = (_id,)
            self.c.execute('select id, retweet_count from fact_status where id = ?', t)
            row = self.c.fetchone()
            if row is None:
                self.fact_status[_id] = [_id, created_at, screen_name, text, tweeter_skey, retweet_count,
                                         in_reply_to_status_id, date_skey, retweet_id, retweet_created_at,
                                         retweet_screen_name, batch_id, '']
                result = 0
            elif retweet_count != row[1]:
                self.fact_status_retweet_count[_id] = retweet_count
                t = (retweet_count, _id)
                self.c.execute('update fact_status set retweet_count = ? where id = ?', t)
            elif retweet_count == row[1]:
                self.fact_status_retweet_count[_id] = retweet_count
        elif _id in self.fact_status_retweet_count and retweet_count != self.fact_status_retweet_count[_id]:
            self.fact_status_retweet_count[_id] = retweet_count
            t = (retweet_count, _id)
            self.c.execute('update fact_status set retweet_count = ? where id = ?', t)

        return result

    def update_tweet_words(self, _id, english_words):
        self.fact_status[_id][12] = english_words

    def write_tweets(self):
        counter = 0
        for key, item in self.fact_status.items():
            # mark_database_activity()
            self.c.execute("""
                insert into fact_status
                 (id, created_at, screen_name, text, tweeter_skey, retweet_count, in_reply_to_status_id, date_skey,
                  retweet_id, retweet_created_at, retweet_screen_name, batch_id, english_words)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?)""", item)
            counter += 1
        logger.info("%i tweets written." % counter)
        self.fact_status = dict()

    def update_max_id(self, hashtag, _id, min_id):
        if _id > 0:
            self.tag_history.append([hashtag, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), _id, min_id])

    def write_tag_history(self):
        counter = 0
        for item in self.tag_history:
            # mark_database_activity()
            self.c.execute('insert into tag_history (tag, date, max_id, min_id) values (?,?,?,?)', item)
            # print "tag_history: %25s,%20s,%20d,%20d" % item
            counter += 1
        logger.info("%i tag histories written." % counter)
        self.tag_history = list()

    def get_trends(self):
        result = {}
        self.c.execute('select tag, result from tag_discovery order by discovery_time')
        row = self.c.fetchone()
        while row is not None:
            result[row[0].lower()] = row[1]
            row = self.c.fetchone()

        logger.info("Fetched trends: %s" % result)

        return result

    def get_trend_discovery_times(self):
        result = {}
        self.c.execute('select tag, max(discovery_time) from tag_discovery group by tag')
        row = self.c.fetchone()
        while row is not None:
            result[row[0].lower()] = row[1]
            row = self.c.fetchone()

        return result

    def set_trend(self, trend, result):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        t = (trend, result, now)
        self.c.execute('INSERT INTO tag_discovery (tag, result, discovery_time) VALUES (?, ?, ?)', t)

    # Tokens
    def load_tokens(self):
        conn_s = sqlite3.connect(self.env.summary_database)
        c_s = conn_s.cursor()
        c_s.execute('SELECT screen_name, token, token_secret from twitter_account')

        rows = c_s.fetchall()

        for sn, t, ts in rows:
            self.tokens.append({'name': sn, 'token': oauth.Token(key=t, secret=ts)})
            if sn == self.env.DEFAULT_ACCOUNT:
                self.default_token_index = len(self.tokens) - 1

        conn_s.close()

    def get_next_token(self):
        self.curr_token_index += 1
        if self.curr_token_index >= len(self.tokens):
            self.curr_token_index = 0

        return self.tokens[self.curr_token_index]['token']

    def get_default_token(self):
        return self.tokens[self.default_token_index]['token']

    def get_all_tokens(self):
        _tokens = []

        for t in self.tokens:
            _tokens.append(t['token'])

        return _tokens

    def load_name_score_data(self):
        if self.name_scores is None:
            self.name_scores = secommon.readCSVHash('data/name_score.csv', removeZero=True)
            self.category_scores = secommon.readCSVHash('data/category_score.csv', removeZero=True)
            self.timezone_scores = secommon.readCSVHash('data/timezone_score.csv', removeZero=True)
            self.location_scores = secommon.readCSVHash('data/location_score.csv', removeZero=True)

            sql = """select screen_name, category
                from dim_tweeter
                where category is not null"""
            self.c.execute(sql)
            rows = self.c.fetchall()
            self.sn_categories = dict()
            for (screen_name, category) in rows:
                self.sn_categories[screen_name] = category

    @staticmethod
    def calculate_text_score(text, score_table):
        result = 0
        words = re.split('[^A-Za-z]+', text.lower())

        for n in words:
            if n in score_table:
                result += score_table[n]

        return result

    @staticmethod
    def none_to_empty(text):
        return text if text is not None else ''

    def get_name_score(self, name, screen_name, location='', timezone=''):
        if self.name_scores is None:
            self.load_name_score_data()

        location = self.none_to_empty(location)
        timezone = self.none_to_empty(timezone)
        category = self.sn_categories[screen_name] if screen_name in self.sn_categories else ''

        name_score = self.calculate_text_score(name, self.name_scores)
        category_score = self.calculate_text_score(category, self.category_scores)
        location_score = self.calculate_text_score(location, self.location_scores)
        timezone_score = self.calculate_text_score(timezone, self.timezone_scores)

        logger.debug("Name:%3d   Category:%3d   Location:%3d   Timezone:%3d  %-20s %s %-15s %-15s" % (
            name_score, category_score, location_score, timezone_score, name[:20], category, location[:15],
            timezone[:15]))

        return category_score + min(10, max(-5, name_score + location_score + timezone_score))

    def get_tag_ranges(self, tag, min_override):
        # tag is a word with preceding hash
        ranges = []
        # since_id = min_override

        t = (tag.lower(),)
        self.c.execute('SELECT min_id, max_id FROM tag_history WHERE tag=? order by max_id desc', t)
        row = self.c.fetchone()
        prev_min = None
        # print "Min Override: %20d,%20d,%20d" % (min_override, row[0], row[1])
        while row is not None and row[0] is not None:
            (min_id, max_id) = row
            if max_id >= min_override:
                if len(ranges) == 0:
                    ranges.append({'tag': tag, 'min_id': max_id, 'max_id': None})
                elif max_id < prev_min:
                    ranges.append({'tag': tag, 'min_id': max_id, 'max_id': prev_min})
            else:
                if len(ranges) == 0:
                    ranges.append({'tag': tag, 'min_id': min_override, 'max_id': None})
                elif max_id < prev_min:
                    ranges.append({'tag': tag, 'min_id': min_override, 'max_id': prev_min})

            prev_min = min_id
            row = self.c.fetchone()

        if prev_min is None or prev_min > min_override:
            ranges.append({'tag': tag, 'min_id': min_override, 'max_id': prev_min})

        return ranges

    def get_tag_completeness(self, tag):
        if self.range_min is None:
            # Get the min and max id ranges for the day
            sql = """select b.min_date, min(id), max(id)
            from fact_status s join db_baseline b
            on substr(s.created_at, 1, 10) = b.min_date"""
            self.c.execute(sql)
            self.min_date, self.range_min, self.range_max = self.c.fetchone()

        ranges = self.get_tag_ranges(tag, self.range_min)
        total_gap = 0
        # print tag
        for r in ranges:
            min_id = r['min_id']
            if min_id < self.range_max:
                max_id = r['max_id'] if r['max_id'] is not None else self.range_max
                total_gap += max_id - min_id

        # print total_gap, max_id, min_id
        return round(1.0 - (float(total_gap) / float(self.range_max - self.range_min)), 3)

    # def update_tweet_words(self, id, english_words):
    #     self.fact_status[id][12] = english_words
