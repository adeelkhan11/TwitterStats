import argparse
import re
# import time
from time import sleep

import twitter

import defaults
import os.path
from twitterstats.secommon import file_timestamp, today
from twitterstats.db import DB, Range
# from time import sleep

from twitterstats.dbsummary import DBSummary
from twitterstats import secommon
# from timeout import timeout, TimeoutError

# 2014-05-02  Fixed self.getTweeterSkey to use lower case screen_name for matching
# 2014-05-05  Updated to scan back before yesterday and update retweet counts on fact_status
# 2014-05-06  Removed favourites_count because it isn't available for tweets
# 2014-05-06  Added id field on dim_tweeter for users twitter id
# 2014-05-10  Added daily hashtag, tag tweeter and tag tag facts
# 2014-05-21  Changed output to single line graph
# 2014-05-21  Added retweet id and retweet created at fields to fact_status
# 2014-05-21  Added table to track number of followers by day
# 2014-05-23  Clean exit on error - batch id for tweets which is deleted
# 2014-05-26  Added request count and tweet count to progress bar
# 2014-05-26  Slows down after 100 requests
# 2014-06-02  Update retweet counts for original tweet and insert if it doesn't exist, same for original tweeter
# 2014-07-07  Saves tweets with Pakistani time (GMT + 4 hours)
# 2014-07-10? Get words and tag information from original tweet rather than truncated retweet text.
# 2014-07-30  Get comma separated list of trends to load rather than single trend
# 2014-07-31  Only store tweeter words for tweeters with category < F
# Copy tweets and other data to new database.
# Retweet friends tweets
# Check for presence of file to kill script
# accounts are hardcoded - read from database instead

import logging

logger = logging.getLogger('words')
MAX_STATUS_ID = 9999999999999999999999


class Words:
    def __init__(self, environment, hashtag, tags_list):
        self.env = environment
        self.hashtag = hashtag

        self.boring_words = {}
        self.banned_tags = {}
        self.data = {}

        self.t_new = 0
        self.t_foreign = 0
        self.t_skip = 0
        self.t_retweet = 0

        # self.ns_tweet_count = []
        # self.ns_total_score = []
        self.ns_score_log = []
        # self.ns_index = -1

        self.retweets = []

        self.date = today()
        self.db = DB(environment, self.date)
        # self.c = self.db.connect(self.date)
        self.db_summary = DBSummary(environment)
        self.load_metadata()

        self.CONSUMER_KEY = self.env.consumer_key
        self.CONSUMER_SECRET = self.env.consumer_secret
        self.current_token = -1

        self.hash_tags_re = re.compile(r'(?i)(?<!\w)#[\w\u064b-\u0657]+', re.UNICODE)

        self.twitters = list()
        for token in self.db_summary.get_all_tokens():
            api = twitter.Api(consumer_key=self.CONSUMER_KEY,
                              consumer_secret=self.CONSUMER_SECRET,
                              access_token_key=token.key,
                              access_token_secret=token.secret)
            self.twitters.append(api)

        self.today_skey = 0

        self.score_names = False
        if hashtag == 'trends':
            if os.path.isfile('metadata/name_score.csv'):
                self.score_names = True
                logger.info("metadata/name_score.csv will be used for name scoring.")
            else:
                logger.info("Warning: metadata/name_score.csv does not exist so name scoring is disabled.")

        self.batch_id = self.db.get_next_batch_id()
        self.baseline_tweet_id = self.db.get_baseline_tweet_id()

        self.today_skey = self.db.get_date_skey(self.date)
        self.loop_pos = -1

        self.all_trends = None
        if hashtag == 'trends':
            self.all_trends = self.db.get_trends()
            self.loop_pos = 0
            if tags_list is None:
                tags_list = []
                for (tag, result) in self.all_trends.items():
                    if result in ('AUTO_ADD', 'MAN_ADD'):
                        tags_list.append({'tag': tag})
            orig_tags_list = tags_list
            tags_list = []
            for tagdata in orig_tags_list:
                tags_list.append(self.db.get_tag_ranges(tagdata['tag'], self.baseline_tweet_id))
            print('Tags_list:', tags_list)
            self.pull_trends(tags_list)
            self.write_data()
        elif hashtag == 'home_timeline':
            status_count = self.pull_data(hashtag)
            logger.info('{} statuses pulled.'.format(status_count))
            self.write_data()
        elif hashtag == 'lists':
            lists = self.twitters[self.db_summary.polling_token_index].GetLists(screen_name=self.env.polling_account)
            logger.info('{} lists for account {}.'.format(len(lists), self.env.polling_account))
            for l in lists:
                status_count = self.pull_data(l.slug)
                logger.info('{} statuses pulled for list {}.'.format(status_count, l.slug))
            self.write_data()

        self.db.disconnect()

    @property
    def api(self):
        self.current_token += 1
        if self.current_token >= len(self.twitters):
            self.current_token = 0
        return self.twitters[self.current_token]

    def pull_trend(self, trend, trend_count, trend_position):
        self.ns_score_log = []
        status_count = 0
        request_count = 0
        index = 0
        while index < len(trend.ranges):
            # for id_range in trend.ranges:
            id_range = trend.ranges[index]
            if not id_range.processed:
                max_id = id_range.max_id
                since_id = id_range.min_id
                statuses = None
                logger.info(
                    'Range: {:>9} {:35} {:20} {:20}'.format(
                        '{:4d}/{:4d}'.format(trend_position, trend_count),
                        trend.name, since_id, 'None' if max_id is None else max_id))
                while statuses is None or len(statuses) >= 50:
                    if request_count >= 100:
                        new_range = Range(min_id=since_id, max_id=max_id)
                        id_range.min_id = max_id
                        trend.ranges.insert(index + 1, new_range)
                        return request_count, status_count, False

                    statuses = self.api.GetSearch(term=trend.name, result_type='recent', count=100,
                                                  include_entities=False, max_id=max_id, since_id=since_id)
                    self.get_words(statuses, trend=trend, source=trend.name)
                    status_count += len(statuses)
                    score = trend.get_average_score(10)
                    if len(statuses) > 0:
                        max_id = statuses[-1].id - 1
                    if id_range.max_id is None and len(statuses) > 0:
                        id_range.max_id = statuses[0].id
                    id_range.processed = True
                    request_count += 1

                    logger.info('{:40}  {:20} {:20} {:3} {:5} {:5.2f} {}'.format(trend.name, since_id,
                                                                                 'None' if max_id is None else max_id,
                                                                                 request_count,
                                                                                 trend.get_status_count(10), score,
                                                                                 trend.state))

                    if score < 0.0 and status_count > 150:
                        trend.state = 'AUTO_DEL'
                        self.save_score_log(self.ns_score_log, trend.name, 'Negative')
                        id_range.min_id = max_id
                        return request_count, status_count, True

                    if score < 0.5 and status_count > 1000:
                        trend.state = 'AUTO_DEL'
                        self.save_score_log(self.ns_score_log, trend.name, 'Hot_Ambiguous')
                        id_range.min_id = max_id
                        return request_count, status_count, True

                    if score > 2.0:
                        if trend.state not in ('AUTO_ADD', 'MAN_ADD'):
                            trend.state = 'AUTO_ADD'

                    # after 500 tweets if we still haven't got an indication, give up
                    if trend.get_status_count(10) > 500 and trend.state == 'AUTO_DEL':
                        self.save_score_log(self.ns_score_log, trend.name, 'Ambiguous')
                        id_range.min_id = max_id
                        return request_count, status_count, True

                    # Not needed for raspberry pi
                    if request_count % 100 == 0:
                        logger.info(f'Sleeping 20 seconds at {request_count} requests.')
                        sleep(20)
            index += 1

        return request_count, status_count, True

    def pull_trends(self, trends):
        total_status_count = 0
        last_write = 0
        total_request_count = 0
        trend_count = len(trends)
        for i, trend in enumerate(trends):
            completed = False
            while not completed:
                request_count, status_count, completed = self.pull_trend(trend, trend_count, i + 1)
                total_request_count += request_count
                total_status_count += status_count
                self.db.tag_history.append(trend)
                if total_request_count >= last_write + 20:
                    self.write_data()
                    self.batch_id += 1
                    last_write = total_request_count
        return total_status_count

    def load_metadata(self):
        f = open('metadata/boring.txt', 'r')
        for line in f:
            self.boring_words[line.rstrip()] = 1
        f.close()

        f = open('metadata/banned_tags.txt', 'r')
        for line in f:
            self.banned_tags[line.rstrip()] = 1
        f.close()

    def cleanup_exit(self):
        logger.info("Deleting batch %i" % self.batch_id)
        self.db.delete_batch(self.batch_id)
        self.db.disconnect()
        exit(1)

    def twitter_search(self, q, sinceid, maxid):
        self.current_token += 1
        if self.current_token >= len(self.twitters):
            self.current_token = 0
        if maxid is None:
            result = self.twitters[self.current_token].GetSearch(term=q, result_type='recent', count='100',
                                                                 include_entities='false', since_id=sinceid)
        else:
            result = self.twitters[self.current_token].GetSearch(term=q, result_type='recent', count='100',
                                                                 include_entities='false', since_id=sinceid,
                                                                 max_id=maxid)
        return result

    # def oauthReq(self, url, key, secret, http_method="GET", post_body='',
    #              http_headers=None):
    #     consumer = oauth.Consumer(key=self.CONSUMER_KEY, secret=self.CONSUMER_SECRET)
    #     if self.hashtag == 'home_timeline':
    #         token = self.db.getDefaultToken()
    #     else:
    #         token = self.db.getNextToken()
    #     client = oauth.Client(consumer, token)
    #     resp, content = client.request(
    #         url,
    #         method=http_method,
    #         body=post_body,
    #         headers=http_headers  # , force_auth_header=True
    #     )
    #     #	print "*** %s ***" % content
    #     #	exit()
    #     return content

    def write_data(self):
        logger.info("Writing data.")

        for tweetdate, stats in self.data.items():
            logger.info("Saving data for %s." % tweetdate)
            for tag, words in self.data[tweetdate]['tag_words'].items():
                for i, v in words.items():
                    self.db.write_hashtag_word(tweetdate, tag, i, v)

            for tweeter, words in self.data[tweetdate]['tweeter_words'].items():
                for i, v in words.items():
                    self.db.write_tweeter_word(tweetdate, tweeter, i, v)

            for tweeter, words in self.data[tweetdate]['tweeter_mentions'].items():
                for i, v in words.items():
                    self.db.write_tweeter_mention(tweetdate, tweeter, i, v)

            for tag, count in self.data[tweetdate]['tags'].items():
                self.db.write_tag(tweetdate, tag, count)

            for tag, tweeters in self.data[tweetdate]['tag_tweeters'].items():
                for i, v in tweeters.items():
                    self.db.write_tag_tweeter(tweetdate, tag, i, v)

            for tag, tags in self.data[tweetdate]['tag_tags'].items():
                for i, v in tags.items():
                    self.db.write_tag_tag(tweetdate, tag, i, v)

        self.db.write_tweets()
        self.db.write_tag_history()

        self.db.commit()
        logger.info("Data saved.")
        self.data = {}

    @staticmethod
    def save_score_log(score_log, trend, reject_reason):
        filename = "log/reject_%s_%s_%s.log" % (trend, reject_reason, file_timestamp())
        secommon.save_list(score_log, filename)

    def calculate_name_score(self, status, trend):
        tweeter = status.user.screen_name
        tweeter_name = status.user.name
        score_candidate = (
            tweeter_name if status.retweeted_status is None else status.retweeted_status.user.name)
        score_candidate_sn = (
            tweeter if status.retweeted_status is None else status.retweeted_status.user.screen_name)
        trend.name_scores[-1].status_count += 1
        # self.ns_tweet_count[self.ns_index] += 1
        name_score = self.db.get_name_score(score_candidate, score_candidate_sn,
                                            status.user.location, status.user.time_zone)
        # self.ns_total_score[self.ns_index] += name_score
        trend.name_scores[-1].total_score += name_score
        score3 = '{:.2f}'.format(trend.get_average_score(3))
        score6 = '{:.2f}'.format(trend.get_average_score(6))
        self.ns_score_log.append(
            [score_candidate, score_candidate_sn, trend.name_scores[-1].status_count, name_score,
             trend.name_scores[-1].total_score, score3, score6, tweeter_name, tweeter, status.id])

    def process_status_words(self, status_id, status_date, status_text, tweeter):
        if status_date not in self.data:
            self.data[status_date] = {}
            self.data[status_date]['tweeter_mentions'] = {}
            self.data[status_date]['tag_words'] = {}
            self.data[status_date]['tweeter_words'] = {}
            self.data[status_date]['tags'] = {}
            self.data[status_date]['tag_tweeters'] = {}
            self.data[status_date]['tag_tags'] = {}

        # get all relevant hashtags
        relevant_hashtags = re.findall(r'(?<![A-Za-z0-9_])#([A-Za-z0-9_]+)', status_text.lower())

        tweet_hashtags = set(self.hash_tags_re.findall(status_text))
        tweet_tags = [ht[1:] for ht in tweet_hashtags]

        for tag in tweet_tags:
            if tag in self.data[status_date]['tags']:
                self.data[status_date]['tags'][tag] += 1
            else:
                self.data[status_date]['tags'][tag] = 1

            if tag not in self.data[status_date]['tag_tweeters']:
                self.data[status_date]['tag_tweeters'][tag] = {}
            if tweeter in self.data[status_date]['tag_tweeters'][tag]:
                self.data[status_date]['tag_tweeters'][tag][tweeter] += 1
            else:
                self.data[status_date]['tag_tweeters'][tag][tweeter] = 1

            if tag not in self.data[status_date]['tag_tags']:
                self.data[status_date]['tag_tags'][tag] = {}
            for tag2 in tweet_tags:
                if tag2 != tag:
                    if tag2 in self.data[status_date]['tag_tags'][tag]:
                        self.data[status_date]['tag_tags'][tag][tag2] += 1
                    else:
                        self.data[status_date]['tag_tags'][tag][tag2] = 1

        # remove links
        text = re.sub(r"(?<![A-Za-z0-9_])https?://[^ ,;'()\[\]<>{}]+", '', status_text, flags=re.IGNORECASE)

        alist = re.split('[, .;\'\"(){\}\[\]<>:?/=+\\\`~!#^&*\\r\\n\-]+', text)
        tweetwords = list()
        for item in alist:
            nitem = item.strip(' ,.-+()[]:\'\"').lower()
            if u"\u2026" in nitem:  # ignore words truncated with ellipsis (...)
                continue
            if nitem == '':
                continue
            if nitem in self.boring_words:
                continue
            if nitem[:1] == '@' and len(nitem) > 2:
                # Tweeter mentions
                if tweeter not in self.data[status_date]['tweeter_mentions']:
                    self.data[status_date]['tweeter_mentions'][tweeter] = {}
                if nitem[1:] in self.data[status_date]['tweeter_mentions'][tweeter]:
                    self.data[status_date]['tweeter_mentions'][tweeter][nitem[1:]] += 1
                else:
                    self.data[status_date]['tweeter_mentions'][tweeter][nitem[1:]] = 1
                continue

            tweetwords.append(nitem)
            for tag in relevant_hashtags:
                if tag not in self.data[status_date]['tag_words']:
                    self.data[status_date]['tag_words'][tag] = {}
                if nitem in self.data[status_date]['tag_words'][tag]:
                    self.data[status_date]['tag_words'][tag][nitem] += 1
                else:
                    self.data[status_date]['tag_words'][tag][nitem] = 1

            # Tweeter words
            if tweeter.lower() in self.db.rated_tweeters:
                if tweeter not in self.data[status_date]['tweeter_words']:
                    self.data[status_date]['tweeter_words'][tweeter] = {}
                if nitem in self.data[status_date]['tweeter_words'][tweeter]:
                    self.data[status_date]['tweeter_words'][tweeter][nitem] += 1
                else:
                    self.data[status_date]['tweeter_words'][tweeter][nitem] = 1

        tweet_words_text = u'~' + u'~'.join(
            [self.db.get_word_skey(x, self.date)[1] for x in sorted(set(tweetwords))]) + u'~'
        self.db.update_tweet_words(status_id, tweet_words_text)

    def get_words(self, statuses, trend=None, source=None):
        # max_id = 0
        # min_id = MAX_STATUS_ID
        for status in statuses:
            # max_id = max(status.id, max_id)
            # min_id = min(status.id, min_id)

            tweeter = status.user.screen_name
            tweeter_name = status.user.name
            tweeter_created_at = self.env.get_local_date(status.user.created_at)
            tweeter_skey = self.db.get_tweeter_skey(screen_name=tweeter,
                                                    name=tweeter_name,
                                                    followers_count=status.user.followers_count,
                                                    friends_count=status.user.friends_count,
                                                    lang=status.user.lang,
                                                    time_zone=status.user.time_zone,
                                                    verified=status.user.verified,
                                                    statuses_count=status.user.statuses_count,
                                                    profile_image_url=status.user.profile_image_url,
                                                    created_at=tweeter_created_at,
                                                    location=status.user.location)

            tweet_text = status.text
            retweet_id = status.retweeted_status.id if status.retweeted_status is not None else 0

            if retweet_id != 0:
                tweet_text = "RT " + status.retweeted_status.user.screen_name + ": " + \
                             status.retweeted_status.text

            if self.score_names:
                self.calculate_name_score(status, trend)

            if status.user.followers_count > 0 and tweeter.lower() in self.db.rated_tweeters:
                self.db.write_daily_followers(tweeter_skey, self.today_skey, status.user.followers_count)

            retweet_created_at = ''
            retweet_screen_name = ''
            retweet_count = status.retweet_count
            if status.retweeted_status is not None:
                self.retweets.append(status.retweeted_status)

                retweet_created_at = self.env.get_local_timestamp(status.retweeted_status.created_at)
                retweet_screen_name = status.retweeted_status.user.screen_name

            # check if duplicate and insert if not duplicate
            status_date = self.env.get_local_date(status.created_at)
            status_created_at = self.env.get_local_timestamp(status.created_at)
            date_skey = self.db.get_date_skey(status_date)
            if self.db.tweet_is_duplicate(id_=status.id,
                                          created_at=status_created_at,
                                          screen_name=status.user.screen_name,
                                          text=tweet_text,
                                          tweeter_skey=tweeter_skey,
                                          retweet_count=retweet_count,
                                          in_reply_to_status_id=status.in_reply_to_status_id,
                                          date_skey=date_skey,
                                          retweet_id=retweet_id,
                                          retweet_created_at=retweet_created_at,
                                          retweet_screen_name=retweet_screen_name,
                                          batch_id=self.batch_id,
                                          source=source):
                self.t_skip += 1
                continue

            self.process_status_words(status_id=status.id,
                                      status_date=status_date,
                                      status_text=tweet_text,
                                      tweeter=tweeter)

    # @timeout(7)
    def pull_data(self, list_name):
        since_id = self.db.get_baseline_tweet_id()
        max_id = None
        all_statuses = []
        statuses = None
        while statuses is None or len(statuses) > 0:
            if list_name == 'home_timeline':
                statuses = self.twitters[self.db_summary.default_token_index].GetHomeTimeline(count=200,
                                                                                              since_id=since_id,
                                                                                              max_id=max_id,
                                                                                              include_entities=False)
            else:
                statuses = self.twitters[self.db_summary.polling_token_index].GetListTimeline(
                    owner_screen_name=self.env.polling_account,
                    slug=list_name,
                    count=200,
                    since_id=since_id,
                    max_id=max_id,
                    include_entities=False)

            if len(statuses) > 0:
                self.get_words(statuses, source=list_name)
                all_statuses.extend(statuses)
                max_id = statuses[-1].id - 1
                logger.info('{}  {}'.format(statuses[-1].id, len(statuses)))

        if len(all_statuses) > 0:
            max_id = max([status.id for status in all_statuses])
            min_id = min([status.id for status in all_statuses])
            self.db.write_list_max_id(list_name, max_id, min_id)

        return len(all_statuses)


def main():
    env = defaults.get_environment()
    parser = argparse.ArgumentParser(description='Download Twitter data.')
    parser.add_argument('command',
                        help='trends, lists or comma separated hashtags')
    args = parser.parse_args()

    if args.command in ('trends', 'lists', 'home_timeline'):
        command = args.command
        tags_list = None
        logger.info('Trendlist')
    else:
        command = 'trends'
        tags_list = [{'tag': '#{}'.format(t)} for t in args.command.split(',')]

    # database = env.database
    _ = Words(env, command, tags_list)


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
