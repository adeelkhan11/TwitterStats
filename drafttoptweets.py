import defaults
from twitterstats.db import DB
from twitterstats.dbsummary import DBSummary
from datetime import timedelta
import datetime
import argparse

# Gets the list of tweets to retweet with stats
# from twitterstats.publisher import Publisher


class TopTweets:
    def __init__(self, db, db_summary, start_date, end_date):
        self.db = db
        self.db_summary = db_summary
        self.start_date = start_date
        self.end_date = end_date
        self._todays_retweets = self._get_todays_retweets()

        for sn, tweets in self._todays_retweets.items():
            print(f'Tweets by {sn}:')
            for t in tweets:
                print(f' - Tweet RT {t.retweet_count:7} {t.text[:40]}')

        rows = db.get_top_tweets(start_date, end_date)
        print('{} rows returned.'.format(len(rows)))
        foreign_rows = db.get_top_foreign_tweets(start_date, end_date)
        print('{} foreign rows returned.'.format(len(foreign_rows)))
        rows.extend(foreign_rows)

        tweet_counts = {}
        main_tweets = []
        celeb_tweets = []
        foreign_main_tweets = []
        foreign_celeb_tweets = []
        for tweet in rows:
            if (db_summary.not_selected_for_retweet(tweet.id)
                    and tweet.screen_name not in tweet_counts
                    and self.retweet_qualified(tweet)):
                tweet_counts[tweet.screen_name] = 1
                if tweet.category == 'A' and len(main_tweets) < 5:
                    main_tweets.append(tweet)
                elif tweet.category == 'B' and len(celeb_tweets) < 3:
                    celeb_tweets.append(tweet)
                elif tweet.category == 'FA' and len(foreign_main_tweets) < 1:
                    foreign_main_tweets.append(tweet)
                elif tweet.category == 'FB' and len(foreign_celeb_tweets) < 1:
                    foreign_celeb_tweets.append(tweet)

        main_tweets = main_tweets[:3]
        main_tweets.extend(celeb_tweets[:2])
        main_tweets.extend(foreign_main_tweets)
        main_tweets.extend(foreign_celeb_tweets)

        self.tweets = main_tweets
        print('{} tweets selected.'.format(len(main_tweets)))

    def retweet_qualified(self, tweet):
        result = False
        rt_daily_limit = self.db.get_daily_retweet_limit(tweet.screen_name)
        print(f'{tweet.screen_name} posted {self.get_todays_retweet_count(tweet.screen_name)} out of {rt_daily_limit} tweets today.')
        if tweet.screen_name in self._todays_retweets:
            for t in self._todays_retweets[tweet.screen_name]:
                print(f' - Tweet {tweet.id} RT {t.retweet_count:7} {t.text[:40]}')
        if self.get_todays_retweet_count(tweet.screen_name) < rt_daily_limit:
            result = True
        else:
            if tweet.retweet_count > self._todays_retweets[tweet.screen_name][-1].retweet_count * 1.1:
                result = True
        print(f'Tweet {tweet.id} by {tweet.screen_name} "{tweet.text[:20]}..." with RTs {tweet.retweet_count} qualified: {result}')
        return result

    # If tweep has already reached daily limit then unpublish the lowest RT tweet
    def retweet_supersede(self, tweet):
        if self.get_todays_retweet_count(tweet.screen_name) >= self.db.get_daily_retweet_limit(tweet.screen_name):
            self.db_summary.save_tweet_posted_status(
                f'{self._todays_retweets[tweet.screen_name][-1].id}-{self.db.env.default_account}',
                'pend-unpost')

    def _get_todays_retweets(self):
        retweet_ids = self.db_summary.get_selected_for_retweet_since_id(self.db.get_baseline_tweet_id(),
                                                                        self.db.min_date)
        tweets = self.db.get_tweets_by_ids(retweet_ids)
        tweeps = dict()
        sorted_tweeps = dict()
        for tweet in tweets:
            if tweet.screen_name not in tweeps:
                tweeps[tweet.screen_name] = list()
            tweeps[tweet.screen_name].append(tweet)

        for screen_name, tweep_tweets in tweeps.items():
            sorted_x = sorted(tweep_tweets, key=lambda x: x.retweet_count, reverse=True)
            sorted_tweeps[screen_name] = sorted_x

        return sorted_tweeps

    def get_todays_retweet_count(self, screen_name):
        if self._todays_retweets is None:
            self._todays_retweets = self._get_todays_retweets()
        return 0 if screen_name not in self._todays_retweets else len(self._todays_retweets[screen_name])


def main(hours):
    with DB(env, datetime.datetime.now().strftime('%Y-%m-%d')) as db:
        db_summary = DBSummary(env)

        start_date = db.datetime_to_str(db.now() - timedelta(hours=hours))
        end_date = db.datetime_to_str(db.now())
        print("Dates: {} ; {}".format(start_date, end_date))

        tt = TopTweets(db, db_summary, start_date, end_date)

        drafted_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            sorted_x = sorted(tt.tweets, key=lambda x: x.id)
            for tweet in sorted_x:
                tt.retweet_supersede(tweet)
                db_summary.save_tweet_retweet(tweet, env.default_account, drafted_date)
            db.set_retweeted(sorted_x)
        except TypeError:
            for t in tt.tweets:
                print(t)
            raise

        db_summary.disconnect()


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))

    env = defaults.get_environment()

    parser = argparse.ArgumentParser(description='Drafts the most popular tweets in the specified time range.')
    parser.add_argument('--hours', default=5, type=int, help='time range in hours')

    args = parser.parse_args()
    main(args.hours)
