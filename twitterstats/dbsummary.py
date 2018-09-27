import sqlite3

from twitterstats.dbutil import DBUtil


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

    def not_retweeted(self, retweet_id):
        t = ('PakPolStats', retweet_id)
        self.c.execute('select tweet_id from retweets where account = ? and tweet_id = ?', t)
        t = self.c.fetchone()
        if t is None:
            return 1
        else:
            return 0
