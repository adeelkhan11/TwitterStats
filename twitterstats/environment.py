from datetime import datetime, timedelta


class Environment:
    BASE_PATH = 'env/'

    def __init__(self, environment, base_url, publishing_account, polling_account, consumer_key, consumer_secret):
        self.database = '{}{}/data/ts.db'.format(self.BASE_PATH, environment)
        self.database_temp = '{}{}/data/ts_temp.db'.format(self.BASE_PATH, environment)
        self.database_old = '{}{}/data/ts_{{}}.db'.format(self.BASE_PATH, environment)
        self.dimension_database = '{}{}/data/ts_dimension.db'.format(self.BASE_PATH, environment)
        self.dimension_database_temp = '{}{}/data/ts_dimension_temp.db'.format(self.BASE_PATH, environment)
        self.dimension_database_old = '{}{}/data/ts_dimension_{{}}.db'.format(self.BASE_PATH, environment)
        self.summary_database = '{}{}/data/ts_summary.db'.format(self.BASE_PATH, environment)
        self.temp_file_directory = '{}{}/scratch'.format(self.BASE_PATH, environment)
        self.bot_data_directory = f'{self.BASE_PATH}{environment}/bot_data'
        self.base_url = base_url
        self.default_account = publishing_account
        self.polling_account = polling_account

        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret

        self.cutoff_a = [125, 400, 800]
        self.cutoff_b = [500, 1000]
        self.cutoff_default = [500]

        self.category_limit_c = 100
        self.category_limit_default = 300

        self.time_zone = 5

        self.post = 'post'
        self.tweet_delay = 55
        self.production = True

        self.lists = dict()

    def get_local_date(self, twitter_time):
        return (datetime.strptime(
            twitter_time.replace(' +0000', ''),
            "%a %b %d %H:%M:%S %Y") + timedelta(
            hours=self.time_zone)).strftime(
            '%Y-%m-%d')

    def get_local_timestamp(self, twitter_time):
        return (datetime.strptime(
            twitter_time.replace(' +0000', ''),
            "%a %b %d %H:%M:%S %Y") + timedelta(
            hours=self.time_zone)).strftime(
            '%Y-%m-%d %H:%M:%S')
