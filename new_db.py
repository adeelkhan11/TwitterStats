# Instead of starting new database, it copies data since the date provided so it doesn't need to be loaded again.
import argparse

import sqlite3
from datetime import timedelta, datetime as time

import defaults
from twitterstats.db import DB
import logging

from twitterstats.dbcopy import DBCopy

logger = logging.getLogger('new_db')


# if len(sys.argv) < 3:
# 	print "Incorrect number of arguments."
# 	exit(1)
#
# database = sys.argv[1]
# new_database = sys.argv[2]
# date = sys.argv[3]


class NewDB:
    def __init__(self, environment, old_date, new_database, date):
        self.environment = environment
        self.old_db = DB(environment, old_date)

        # don't connect to new db via regular function, the global connection on db.py should be the main db
        conn2 = sqlite3.connect(new_database)
        self.new_c = conn2.cursor()
        
        db_copy = DBCopy(self.old_db.c, self.new_c)

        if date == 'empty':
            self.old_db.disconnect()
            conn2.commit()
            conn2.close()
            return

        datedt = time.strptime(date, '%Y-%m-%d')
        threedaysago = (datedt - timedelta(days=3)).strftime('%Y-%m-%d')
        recentdate = (datedt - timedelta(days=7)).strftime('%Y-%m-%d')

        logger.info('Recentdate: [' + recentdate + ']')

        date_skey = self.old_db.get_date_skey(date)

        # Special case for db_baseline
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
        logger.info(f'{cnt} rows')

        # The remaining tables
        sql = """select {} from tag_history th
                 where max_id >= ?"""
        t = (baseline_id,)
        db_copy.copy_table('tag_history', sql, t)

        sql = """select {} from (select d.*
            from tag_discovery d
            left join tag_score s
            on d.tag = s.tag
            group by d.tag, d.result, d.discovery_time
            having discovery_time >= ? or sum(s.tweet_count) > ?)"""
        t = (threedaysago, 50)
        db_copy.copy_table('tag_discovery', sql, t)

        sql = """select {}
            from tag_score
            where score_time >= ?"""
        t = (date,)
        db_copy.copy_table('tag_score', sql, t)

        # FACT_DAILY_FOLLOWERS
        sql = 'select {} from fact_daily_followers where date_skey >= ?'
        t = (date_skey,)
        db_copy.copy_table('fact_daily_followers', sql, t)

        # FACT_DAILY_HASHTAG
        sql = 'select {} from fact_daily_hashtag where date_skey >= ?'
        t = (date_skey,)
        db_copy.copy_table('fact_daily_hashtag', sql, t)

        sql = 'select {} from fact_daily_hashtag_hashtag where date_skey >= ?'
        t = (date_skey,)
        db_copy.copy_table('fact_daily_hashtag_hashtag', sql, t)

        sql = 'select {} from fact_daily_hashtag_tweeter where date_skey >= ?'
        t = (date_skey,)
        db_copy.copy_table('fact_daily_hashtag_tweeter', sql, t)

        sql = 'select {} from fact_daily_hashtag_word where date_skey >= ?'
        t = (date_skey,)
        db_copy.copy_table('fact_daily_hashtag_word', sql, t)

        sql = """select {}
            from fact_daily_tweeter_mention where date_skey >= ?"""
        t = (date_skey,)
        db_copy.copy_table('fact_daily_tweeter_mention', sql, t)

        sql = 'select {} from fact_daily_tweeter_word where date_skey >= ?'
        t = (date_skey,)
        db_copy.copy_table('fact_daily_tweeter_word', sql, t)

        # FACT_STATUS RETWEETED
        sql = 'select {} from fact_status where date_skey = ? and retweeted is not null'
        t = (date_skey - 1,)
        db_copy.copy_table('fact_status', sql, t)

        # FACT_STATUS
        sql = 'select {} from fact_status where date_skey >= ?'
        t = (date_skey,)
        db_copy.copy_table('fact_status', sql, t)

        self.old_db.disconnect()
        conn2.commit()
        conn2.close()


def main():
    env = defaults.get_environment()
    parser = argparse.ArgumentParser(description='Copy the standard database.')
    parser.add_argument('old_date',
                        help='the date of the current database')
    parser.add_argument('new_database',
                        help='the new database')
    parser.add_argument('date',
                        help='the date from which the new database is active')
    args = parser.parse_args()

    _ = NewDB(env, args.old_date, args.new_database, args.date)


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
