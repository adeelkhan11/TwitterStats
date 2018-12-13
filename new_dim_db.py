# Instead of starting new database, it copies data since the date provided so it doesn't need to be loaded again.
import os
import sqlite3
from datetime import timedelta, datetime

import defaults
from twitterstats.db import DB
import logging

from twitterstats.dbcopy import DBCopy, ArchiveFileExistsError
from twitterstats.secommon import today, last_month

logger = logging.getLogger('new_dim_db')


class NewDimDB:
    def __init__(self, environment, old_date):
        self.environment = environment
        archive_file = environment.dimension_database_old.format(last_month().replace('-', '_'))
        if os.path.isfile(archive_file):
            raise ArchiveFileExistsError()

        self.old_db = sqlite3.connect(environment.dimension_database)
        self.old_c = self.old_db.cursor()
        self.old_c.execute('ATTACH DATABASE ? AS se', (environment.database,))

        date = None
        try:
            self.old_c.execute('select min_date from db_baseline')
            date = self.old_c.fetchone()[0]
        except sqlite3.OperationalError:
            logger.critical('Could not read date.')
            exit(1)

        logger.info('Date: %s', date)

        self.old_db = DB(environment, old_date)

        # don't connect to new db via regular function, the global connection on db.py should be the main db
        conn2 = sqlite3.connect(environment.dimension_database_temp)
        self.new_c = conn2.cursor()
        
        db_copy = DBCopy(self.old_c, self.new_c)

        if date == 'empty':
            self.old_db.disconnect()
            conn2.commit()
            conn2.close()
            return

        date_dt = datetime.strptime(date, '%Y-%m-%d')
        recent_date = (date_dt - timedelta(days=30)).strftime('%Y-%m-%d')
        twitter_dt = datetime.strptime('2006-03-21 09:00:00', '%Y-%m-%d %H:%M:%S')
        future_date = (date_dt + timedelta(days=2000)).strftime('%Y-%m-%d')

        logger.info('Recent date: %s', recent_date)

        logger.info("DIM_DATE")
        cnt = 0
        dt = twitter_dt.strftime('%Y-%m-%d')
        i = 1
        while dt < future_date:
            cnt += 1
            # print "Date: ", i, dt
            t = (i, dt)
            self.new_c.execute('insert into dim_date (date_skey, date) values (?, ?)', t)
            dt = (twitter_dt + timedelta(days=i)).strftime('%Y-%m-%d')
            i += 1
        logger.info('%d rows', cnt)

        # DIM_TWEETER
        t = (recent_date,)
        sql = """select {}
        from dim_tweeter 
        where category is not null
        or ifnull(relevance_score, 0) != 0
        or ifnull(bot_date, '2000-01-01') >= ? 
        or tweeter_skey in (select tweeter_skey from fact_daily_followers
            union select tweeter_skey from fact_daily_hashtag_tweeter
            union select tweeter_skey from fact_daily_tweeter_mention
            union select mentioned_tweeter_skey from fact_daily_tweeter_mention
            union select tweeter_skey from fact_daily_tweeter_word)
        """
        db_copy.copy_table('dim_tweeter', sql, t)

        # DIM_WORD
        # Word skeys were getting very large because we only moved the new days words forward, so many words
        # that weren't used in the few hours of the new day were being created with new skeys. Using yesterday
        # will slow the skey generation.
        sql = """select {}
        from dim_word
        where generic is not null or relevance is not null
        or word_skey in (
            select word_skey from fact_daily_hashtag
            union select tag_skey from fact_daily_hashtag_hashtag
            union select other_tag_skey from fact_daily_hashtag_hashtag
            union select tag_skey from fact_daily_hashtag_tweeter
            union select tag_skey from fact_daily_hashtag_word
            union select word_skey from fact_daily_hashtag_word
            union select word_skey from fact_daily_tweeter_word)"""
        db_copy.copy_table('dim_word', sql, None)

        self.old_db.disconnect()
        conn2.commit()
        conn2.close()

        db_copy.switch_files(current_file=environment.dimension_database,
                             archive_file=archive_file,
                             new_file=environment.dimension_database_temp)


def main():
    env = defaults.get_environment()

    _ = NewDimDB(env, today())


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
