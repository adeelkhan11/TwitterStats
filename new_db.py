# Instead of starting new database, it copies data since the date provided so it doesn't need to be loaded again.
import argparse

import sqlite3
from datetime import timedelta, datetime as time

import defaults
from twitterstats.db import DB
import logging

logger = logging.getLogger('new_db')


# if len(sys.argv) < 3:
# 	print "Incorrect number of arguments."
# 	exit(1)
#
# database = sys.argv[1]
# new_database = sys.argv[2]
# date = sys.argv[3]


class DBCopy:
    def copy_table(self, table_name, column_names, source_query, source_query_parameters):
        logger.info(table_name)
        # self.old_db.c.execute(f'PRAGMA table_info({table_name})')
        # rows = self.old_db.c.fetchall()
        # columns = [r[1] for r in rows]
        # column_names = ', '.join(columns)
        data_markers = ', '.join(['?'] * len(column_names.split(',')))

        cnt = 0
        self.old_db.c.execute(source_query, source_query_parameters)
        row = self.old_db.c.fetchone()
        while row is not None:
            cnt += 1
            t = row
            self.new_c.execute(f'insert into {table_name} ({column_names}) values ({data_markers})', t)
            row = self.old_db.c.fetchone()
        logger.info(f'{cnt} rows')

    def __init__(self, environment, old_date, new_database, date):
        self.environment = environment
        self.old_db = DB(environment, old_date)
        # c = db_connect(database)
        # attach_dimension(c)

        # don't connect to d via regular function, the global connection on db.py should be the main db
        # d = db_connect(new_database)
        conn2 = sqlite3.connect(new_database)
        self.new_c = conn2.cursor()

        # Tables
        t = ('table', 'sqlite_sequence')
        self.old_db.c.execute('select sql from sqlite_master where type = ? and name != ?', t)
        rows = self.old_db.c.fetchall()
        for sql in rows:
            self.new_c.execute(sql[0])

        # Indexes
        t = ('index',)
        self.old_db.c.execute('select sql from sqlite_master where type = ?', t)
        rows = self.old_db.c.fetchall()
        for sql in rows:
            self.new_c.execute(sql[0])

        if date == 'empty':
            self.old_db.disconnect()
            conn2.commit()
            conn2.close()
            return

        datedt = time.strptime(date, '%Y-%m-%d')
        # yesterdaydt = datedt - timedelta(days=1)
        # yesterday = yesterdaydt.strftime('%Y-%m-%d')
        threedaysago = (datedt - timedelta(days=3)).strftime('%Y-%m-%d')
        recentdate = (datedt - timedelta(days=7)).strftime('%Y-%m-%d')
        # twitterdt = time.strptime('2006-03-21 09:00:00', '%Y-%m-%d %H:%M:%S')
        # futuredate = (datedt + timedelta(days=100)).strftime('%Y-%m-%d')

        logger.info('Recentdate: [' + recentdate + ']')

        date_skey = self.old_db.get_date_skey(date)
        # yesterday_skey = self.old_db.get_date_skey(yesterday)

        baseline_id = 0
        logger.info('db_baseline')
        cnt = 0
        sql = """select max(id) from fact_status where date_skey < ?"""
        t = (date_skey,)
        self.old_db.c.execute(sql, t)
        row = self.old_db.c.fetchone()
        if row is not None:
            cnt += 1
            baseline_id = row[0]
            t = (row[0], date)
            self.new_c.execute('insert into db_baseline (min_tweet_id, min_date) values (?, ?)', t)
        logger.info(cnt, 'rows')

        sql = """select tag, date, max_id, min_id from tag_history th
                 where max_id >= ?"""
        t = (baseline_id,)
        self.copy_table('tag_history', 'tag, date, max_id, min_id', sql, t)

        sql = """select d.tag, d.result, d.discovery_time
            from tag_discovery d
            left join tag_score s
            on d.tag = s.tag
            group by d.tag, d.result, d.discovery_time
            having discovery_time >= ? or sum(s.tweet_count) > ?"""
        t = (threedaysago, 50)
        self.copy_table('tag_discovery', 'tag, result, discovery_time', sql, t)

        sql = """select tag, tweet_count, score, max_id, score_time
            from tag_score
            where score_time >= ?"""
        t = (date,)
        self.copy_table('tag_score', 'tag, tweet_count, score, max_id, score_time', sql, t)

        # FACT_DAILY_FOLLOWERS
        sql = 'select tweeter_skey, date_skey, followers_count from fact_daily_followers where date_skey >= ?'
        t = (date_skey,)
        self.copy_table('fact_daily_followers', 'tweeter_skey, date_skey, followers_count', sql, t)

        # FACT_DAILY_HASHTAG
        sql = 'select word_skey, date_skey, hashtag, count from fact_daily_hashtag where date_skey >= ?'
        t = (date_skey,)
        self.copy_table('fact_daily_hashtag', 'word_skey, date_skey, hashtag, count', sql, t)

        sql = 'select tag_skey, other_tag_skey, date_skey, count from fact_daily_hashtag_hashtag where date_skey >= ?'
        t = (date_skey,)
        self.copy_table('fact_daily_hashtag_hashtag', 'tag_skey, other_tag_skey, date_skey, count', sql, t)

        sql = 'select tag_skey, tweeter_skey, date_skey, count from fact_daily_hashtag_tweeter where date_skey >= ?'
        t = (date_skey,)
        self.copy_table('fact_daily_hashtag_tweeter', 'tag_skey, tweeter_skey, date_skey, count', sql, t)

        sql = 'select tag_skey, word_skey, date_skey, count from fact_daily_hashtag_word where date_skey >= ?'
        t = (date_skey,)
        self.copy_table('fact_daily_hashtag_word', 'tag_skey, word_skey, date_skey, count', sql, t)

        sql = """select tweeter_skey, mentioned_tweeter_skey, date_skey, count
            from fact_daily_tweeter_mention where date_skey >= ?"""
        t = (date_skey,)
        self.copy_table('fact_daily_tweeter_mention', 'tweeter_skey, mentioned_tweeter_skey, date_skey, count', sql, t)

        sql = 'select tweeter_skey, word_skey, date_skey, count from fact_daily_tweeter_word where date_skey >= ?'
        t = (date_skey,)
        self.copy_table('fact_daily_tweeter_word', 'tweeter_skey, word_skey, date_skey, count', sql, t)

        # FACT_STATUS RETWEETED
        col_names = 'id, created_at, screen_name, text, tweeter_skey, retweet_count, in_reply_to_status_id, ' \
                    'date_skey, favourites_count, retweet_id, retweet_created_at, batch_id, retweeted, ' \
                    'retweet_screen_name, english_words'

        sql = f'select {col_names} from fact_status where date_skey = ? and retweeted is not null'
        t = (date_skey - 1,)
        self.copy_table('fact_status', col_names, sql, t)

        # FACT_STATUS
        sql = f'select {col_names} from fact_status where date_skey >= ?'
        t = (date_skey,)
        self.copy_table('fact_status', col_names, sql, t)

        self.old_db.disconnect()
        conn2.commit()
        conn2.close()


def main():
    env = defaults.get_environment()
    parser = argparse.ArgumentParser(description='Copy the standard database.')
    parser.add_argument('database',
                        help='the current database')
    parser.add_argument('new_database',
                        help='the new database')
    parser.add_argument('date',
                        help='the date from which the new database is active')
    args = parser.parse_args()

    _ = DBCopy(env, args.database, args.new_database, args.date)


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
