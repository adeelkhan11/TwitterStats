import defaults
from twitterstats.db import DB
from twitterstats.dbsummary import DBSummary
from datetime import timedelta
import datetime
import argparse

# Gets the list of tweets to retweet with stats
from twitterstats.publisher import Publisher

env = defaults.get_environment()

parser = argparse.ArgumentParser(description='Drafts the most popular tweets in the specified time range.')
parser.add_argument('--start_date', default='', help='date in format yyyy-mm-dd hh:mi:ss')
parser.add_argument('--end_date', default='2999-12-31 23:59:59', help='date in format yyyy-mm-dd hh:mi:ss')

args = parser.parse_args()

with DB(env, datetime.datetime.now().strftime('%Y-%m-%d')) as db:
    db_summary = DBSummary(env)

    start_date = (datetime.datetime.now() - timedelta(hours=24)).strftime(
        '%Y-%m-%d %H:%M:%S') if args.start_date == '' else args.start_date
    end_date = args.end_date
    print("Dates: {} ; {}".format(start_date, end_date))

    rows = db.get_top_tweets(start_date, end_date)
    print('{} rows returned.'.format(len(rows)))

    tweet_counts = {}
    main_tweets = []
    celeb_tweets = []
    other_tweets = []
    stranger_tweets = []
    foreign_tweets = []
    tweets = []
    for tweet in rows:
        if db_summary.not_retweeted(tweet.id):
            if tweet.screen_name not in tweet_counts:
                tweet_counts[tweet.screen_name] = 1
                tweet.rank = tweet_counts[tweet.screen_name]
                if tweet.category == 'A' and len(main_tweets) < 15:
                    main_tweets.append(tweet)
                elif tweet.category == 'B' and len(celeb_tweets) < 10:
                    celeb_tweets.append(tweet)
                elif tweet.category in ['C', 'D', 'E']:
                    other_tweets.append(tweet)
                elif tweet.category == 'F':
                    foreign_tweets.append(tweet)
                elif tweet.category is None:
                    stranger_tweets.append(tweet)
                else:
                    tweets.append(tweet)
            else:
                tweet_counts[tweet.screen_name] += 1
                tweet.rank = tweet_counts[tweet.screen_name]
                if (tweet.category == 'A'
                    and tweet_counts[tweet.screen_name] <= len(env.cutoff_a)
                    and tweet.score >= env.cutoff_a[tweet_counts[tweet.screen_name] - 1]
                    ) or (
                        tweet.category == 'B'
                        and tweet_counts[tweet.screen_name] <= len(env.cutoff_b)
                        and tweet.score >= env.cutoff_b[tweet_counts[tweet.screen_name] - 1]):
                    tweets.append(tweet)

    main_tweets.extend(tweets[:10])
    main_tweets.extend(celeb_tweets)
    main_tweets.extend(other_tweets[:5])
    main_tweets.extend(foreign_tweets[:5])
    main_tweets.extend(stranger_tweets[:5])
    print('{} tweets selected.'.format(len(main_tweets)))

    try:
        sorted_x = sorted(main_tweets, key=lambda x: x.id)
    except TypeError as e:
        for t in main_tweets:
            print(t)
        raise

    drafted_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for tweet in sorted_x:
        tweet.drafted_date = drafted_date
        tweet.account = env.default_account

    url = env.base_url + 'deliver'
    data = {'tweets': sorted_x}
    Publisher.publish(url, data, 'draft', env.default_account)
    db.set_retweeted(sorted_x)
