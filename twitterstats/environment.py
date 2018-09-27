

class Environment:
    BASE_PATH = 'env/'

    def __init__(self, environment, base_url, twitter_account, twitter_key, twitter_secret):
        self.database = '{}{}/data/ts.db'.format(self.BASE_PATH, environment)
        self.database_old = '{}{}/data/ts_{{}}.db'.format(self.BASE_PATH, environment)
        self.dimension_database = '{}{}/data/ts_dimension.db'.format(self.BASE_PATH, environment)
        self.dimension_database_old = '{}{}/data/ts_dimension_{{}}.db'.format(self.BASE_PATH, environment)
        self.summary_database = '{}{}/data/ts_summary.db'.format(self.BASE_PATH, environment)
        self.base_url = base_url
        self.default_account = twitter_account

        self.consumer_key = twitter_key
        self.consumer_secret = twitter_secret

        self.cutoff_a = [25, 125, 250]
        self.cutoff_b = [60, 160]
        self.cutoff_default = [120]

        self.category_limit_c = 100
        self.category_limit_default = 300

        self.time_zone = 5

        self.post = 'post'
        self.tweet_delay = 55



