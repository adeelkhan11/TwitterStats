import sqlite3

from twitterstats.dbutil import DBUtil
import oauth2 as oauth


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
