import defaults
# import oauth2 as oauth
import json
# import sqlite3
import sys
import operator
# from db import *
from twitterstats.db import DB
from twitterstats.dbsummary import DBSummary
# from twitterstats.dbutil import DBUtil
from datetime import date, timedelta
import datetime
# import csv, codecs, cStringIO
import urllib
# import urllib2
import argparse

# Gets the list of tweets to retweet with stats
from twitterstats.publisher import Publisher

env = defaults.get_environment()

parser = argparse.ArgumentParser(description='Drafts the most popular tweets in the specified time range.')
parser.add_argument('--start_date', default='', help='date in format yyyy-mm-dd hh:mi:ss')
parser.add_argument('--end_date', default='2999-12-31 23:59:59', help='date in format yyyy-mm-dd hh:mi:ss')

args = parser.parse_args()

#d = None # db_connect(defaults.summary_database)
with DB(env, datetime.datetime.now().strftime('%Y-%m-%d')) as db:
    db_summary = DBSummary(env)
# db = DBUtil(env)
# db.connect(datetime.datetime.now().strftime('%Y-%m-%d'))
# c = db_connect()

    start_date = (datetime.datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S') if args.start_date == '' else args.start_date
    end_date = args.end_date
    print("Dates: {} ; {}".format(start_date, end_date))
    # t = (start_date, end_date, defaults.CUTOFF_A[0], 'A', defaults.CUTOFF_B[0], 'G', defaults.CUTOFF_DEFAULT[0])
    # sql = """select dt.category, t.id, t.created_at, t.retweet_count + (bot.botfactor * 2) as score, t.screen_name, t.text, t.retweet_count, bot.bot_data_availability from (
    # select count(*) tcount, sum(IfNull(bot_score, 4) - 4) as botfactor, sum(case when bot_score is null then 0 else 1 end) bot_data_availability, max(t.retweet_count) retweet_count, retweet_id, t.retweet_screen_name, t.text
    # from fact_status t join dim_tweeter dt on t.tweeter_skey = dt.tweeter_skey
    # where t.retweet_id != 0
    # group by t.retweet_id, t.retweet_screen_name, t.text
    # ) bot
    # join fact_status t on bot.retweet_id = t.id
    # join dim_tweeter dt on dt.screen_name = t.screen_name
    # where t.created_at >= ?
    # and t.created_at <= ?
    # and bot.tcount > 1
    # and t.retweet_id = 0
    # and t.retweeted is null
    # and ((t.retweet_count >= ? and dt.category = ?) or (t.retweet_count >= ? and dt.category < ?) or t.retweet_count > ?)
    # order by t.retweet_count + (bot.botfactor * 2) desc LIMIT 1000
    # ;
    # """
    # db.c.execute(sql, t)
    # rows = db.c.fetchall()

    rows = db.get_top_tweets(start_date, end_date)
    print('{} rows returned.'.format(len(rows)))

    tweetcounts = {}
    maintweets = []
    celebtweets = []
    othertweets = []
    strangertweets = []
    foreigntweets = []
    tweets = []
    for tweet in rows:
        if db_summary.not_retweeted(tweet.id):
            # if row.screen_name == 'ImranKhanPTI':
            #     print "IK:", row.id, row.score
            if tweet.screen_name not in tweetcounts:
                tweetcounts[tweet.screen_name] = 1
                tweet.rank = tweetcounts[tweet.screen_name]
                if tweet.category == 'A' and len(maintweets) < 15:
                    maintweets.append(tweet)
                elif tweet.category == 'B' and len(celebtweets) < 10:
                    celebtweets.append(tweet)
                elif tweet.category in ['C', 'D', 'E']:
                    othertweets.append(tweet)
                elif tweet.category == 'F':
                    foreigntweets.append(tweet)
                elif tweet.category is None:
                    strangertweets.append(tweet)
                else:
                    tweets.append(tweet)
            else:
                tweetcounts[tweet.screen_name] += 1
                tweet.rank = tweetcounts[tweet.screen_name]
                if (tweet.category == 'A'
                    and tweetcounts[tweet.screen_name] <= len(env.cutoff_a)
                    and tweet.score >= env.cutoff_a[tweetcounts[tweet.screen_name] - 1]
                    ) or (
                        tweet.category == 'B'
                        and tweetcounts[tweet.screen_name] <= len(env.cutoff_b)
                        and tweet.score >= env.cutoff_b[tweetcounts[tweet.screen_name] - 1]):
                    tweets.append(tweet)

    # print "Maintweets:", len(maintweets)
    # for (cat, id, at, rc, sn, t, rt, bda, rank) in maintweets:
    #     print cat, id, at, rt, sn, rc, bda

    # print maintweets
    # print "Stranger tweets:", len(strangertweets)
    maintweets.extend(tweets[:10])
    maintweets.extend(celebtweets)
    maintweets.extend(othertweets[:5])
    maintweets.extend(foreigntweets[:5])
    maintweets.extend(strangertweets[:5])
    # print "Maintweets and others:", len(maintweets)
    print('{} tweets selected.'.format(len(maintweets)))

    try:
        sorted_x = sorted(maintweets, key=lambda x: x.id)
    except TypeError as e:
        for t in maintweets:
            print(t)
        raise

    drafted_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for tweet in sorted_x:
        tweet.drafted_date = drafted_date
        tweet.account = env.default_account

    url = env.base_url + 'deliver'
    data = {'tweets': sorted_x}
    # jtext = json.dumps(jdata)
    Publisher.publish(url, data, 'draft', env.default_account)
    db.set_retweeted(sorted_x)

exit()
# cats = dict()
# # for (cat, id, at, rc, sn, t, rt, bda, rank) in maintweets:
# for tweet in maintweets:
#     # print cat, id, at, rt, sn, rc, bda
#     realcat = (tweet.category if tweet.category is not None else "None")
#     if realcat in cats:
#         cats[realcat] += 1
#     else:
#         cats[realcat] = 1

# for key in sorted(cats.keys()):
#     print "%5s %3i" % (key, cats[key])

sorted_x = sorted(maintweets, key=lambda x: x.id)

i = 0
# print "\nTweets:"

drafted_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

tweets = list()
# while i < len(sorted_x):
for tweet in sorted_x:
    # print sorted_x[i][0], sorted_x[i][1], sorted_x[i][2], sorted_x[i][3], sorted_x[i][4]
    # print sorted_x[i][5]
    # (tweeter_type, tid, tcreated_at, tbot_score, tscreen_name, ttext, tretweet_count, tbot_data_availability, rank) = \
    # sorted_x[i]
    bot_score = str((tbot_score - tretweet_count) / 2) + "/" + str(tbot_data_availability)
    tweet = {'tweet_id': tid, 'type': 'retweet', 'tweeter_type': tweeter_type, 'tweet_created_at': tcreated_at,
             'tweet_retweet_count': tretweet_count, 'bot_score': bot_score, 'rank': rank,
             'tweet_screen_name': tscreen_name, 'head': ttext, 'drafted_at': drafted_date,
             'account': defaults.DEFAULT_ACCOUNT}
    tweets.append(tweet)
    t = ('Y', tid)
    # c.execute('update fact_status set retweeted = ? where id = ?', t)
    i += 1

jdata = {'tweets': tweets}
jtext = json.dumps(jdata)
# print len(jtext), jtext

url = defaults.base_url + 'deliver'
values = {'data': jtext,
          'type': 'draft',
          'account': defaults.DEFAULT_ACCOUNT}

# data = urllib.urlencode(values)
# data = data.encode('utf-8') # data should be bytes
# req = urllib2.Request(url, data)
# response = urllib2.urlopen(req)
# the_page = response.read()

# print '---'
# print the_page

# db_commit()
# db_disconnect()
