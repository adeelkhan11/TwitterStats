import logging
import sqlite3
from twitterstats.dbutil import DBUtil, PublishTweet

logger = logging.getLogger(__name__)


class DB(DBUtil):
    def __init__(self, environment, date):
        DBUtil.__init__(self, environment)
        self.tokens = []
        self.curr_token_index = -1
        self.default_token_index = -1

        self.fact_status = dict()
        self.tag_history = list()
        self.date_skey_cache = dict()
        self.fact_status_retweet_count = dict()
        self.name_scores = None
        self.category_scores = None
        self.timezone_scores = None
        self.location_scores = None
        self.sn_categories = None

        self.min_date = None
        self.range_min = None
        self.range_max = None

        self.connect(date)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.disconnect()

    def connect(self, date):
        database = self.se_database('se', date)
        dim_database = self.se_database('se_dimension', date)
        print('Databases: {},  {}'.format(database, dim_database))
        self.conn = sqlite3.connect(database)
        self.c = self.conn.cursor()
        self.c.execute('ATTACH DATABASE ? AS dim', (dim_database,))
        self.c.execute('SELECT min_date from db_baseline')
        min_date = self.c.fetchone()[0]
        if min_date != date and not (min_date < date and database == self.env.database):
            logger.critical("Could not find database for date %s", date)
            exit(1)
        # return self.c

    def set_retweeted(self, tweets):
        for tweet in tweets:
            t = ('Y', tweet.id)
            self.c.execute('update fact_status set retweeted = ? where id = ?', t)
        self.commit()

    def get_top_tweets(self, start_date, end_date):
        t = (start_date, end_date, self.env.cutoff_a[0], 'A', self.env.cutoff_b[0], 'G', self.env.cutoff_default[0])
        sql = """select t.id, t.created_at, t.screen_name,
        t.text, t.retweet_count, dt.category, t.retweet_count + (bot.bot_factor * 2) as score, bot.bot_data_availability from (
        select count(*) tweet_count, sum(IfNull(bot_score, 4) - 4) as bot_factor,
        sum(case when bot_score is null then 0 else 1 end) bot_data_availability, max(t.retweet_count) retweet_count,
        retweet_id, t.retweet_screen_name, t.text
        from fact_status t join dim_tweeter dt on t.tweeter_skey = dt.tweeter_skey
        where t.retweet_id != 0
        group by t.retweet_id, t.retweet_screen_name, t.text
        ) bot
        join fact_status t on bot.retweet_id = t.id
        join dim_tweeter dt on dt.screen_name = t.screen_name
        where t.created_at >= ?
        and t.created_at <= ?
        and bot.tweet_count > 1
        and t.retweet_id = 0
        and t.retweeted is null
        and ((t.retweet_count >= ? and dt.category = ?) or (t.retweet_count >= ? and dt.category < ?)
        or t.retweet_count > ?)
        order by t.retweet_count + (bot.bot_factor * 2) desc LIMIT 1000
        ;
        """
        self.c.execute(sql, t)
        rows = self.c.fetchall()

        tweets = [PublishTweet(*row) for row in rows]
        return tweets
