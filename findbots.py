import argparse
import logging
import defaults
import operator
import re
import datetime

from twitterstats.db import DB


class FindBots:
    def __init__(self, db, date_to):
        self.db = db
        self.bot_date = date_to
        date_to_dtm = datetime.datetime.strptime(date_to, '%Y-%m-%d')

        rt_dict = dict()

        logger.info(f'Processing {date_to}')
        # Clear bot date and update tweet_date
        self.refresh_dim_tweeter(date_to)
        # Get tweeters that RTed today
        # snlist = get_suspects(date_to)

        # Get details of users who RTed today
        rt_dict = self.get_rt_variance(date_to, rt_dict)

        bot_scores = list()

        for name, data in rt_dict.items():
            score = 0
            scores = list()
            # 1 tweeted today
            if data['tweet_date'] is not None:
                if data['tweet_date'][:1] == 'X':
                    score += 1
                    scores.append('TToday')
                # 1 tweeted earlier this week
                if data['tweet_date'][1:].strip() != '':
                    score += 1
                    scores.append('TWeek')
            # 1 account older than a week
            if data['created_at'] is not None:
                created_at = datetime.datetime.strptime(data['created_at'], '%Y-%m-%d')
                age = (date_to_dtm - created_at).days
                if age > 90:
                    score += 1
                    scores.append('Old90')
                # 1 account older than 3 months
                if age > 365:
                    score += 1
                    scores.append('Old365')
                # 1 account older than 1 year
                if age > 1000:
                    score += 1
                    scores.append('Old1000')
            # 1 follows me -- can't use tweet data because it is retrieved by different accounts
            # 1 rt 5 or more people
            if data['rt'] is not None:
                if data['rt'] >= 5:
                    score += 1
                    scores.append('RT5')
            # 3 verified   -- not much value as accounts are rarely verified
            # 2 followers > max(following, 30)
            if data['followers'] is not None and data['friends'] is not None:
                if data['followers'] > data['friends'] and data['followers'] > 30:
                    score += 1
                    scores.append('Followers30')
            # 2 followers > max(following, 30)
            if data['followers'] is not None and data['friends'] is not None:
                if data['friends'] > 30:
                    ff_ratio = data['followers'] / data['friends']
                    if ff_ratio >= 1.0:
                        score += 1
                        scores.append('Followersx1')
                    if ff_ratio >= 5.0:
                        score += 1
                        scores.append('Followersx5')
                    if ff_ratio >= 10.0:
                        score += 1
                        scores.append('Followersx10')
                    if ff_ratio <= 0.2:
                        score -= 1
                        scores.append('Followers/5')
                    if ff_ratio <= 0.1:
                        score -= 1
                        scores.append('Followers/10')
                    if abs(ff_ratio - 1.0) < 0.2:
                        score -= 1
                        scores.append('FollowBacker')
                    if abs(ff_ratio - 1.0) < 0.1:
                        score -= 1
                        scores.append('FollowBackerPlus')
            # 1 screen_name shorter than 15
            if len(name) < 15:
                score += 1
                scores.append('ShortName15')
            # 1 screen_name has less than 4 non-alphabet characters
            if len(re.sub('[a-zA-Z]', '', name)) < 4:
                score += 1
                scores.append('CrypticName4')
            # 2 category < C
            if data['category'] is not None:
                if data['category'] < 'C':
                    score += 2
                    scores.append('CatB')
                # 1 category < D
                if data['category'] < 'D':
                    score += 1
                    scores.append('CatC')
                if data['category'] == 'R':
                    score = -2
                    scores.append('CatR')
            bot_scores.append([name, score, scores])

        sorted_x = sorted(bot_scores, key=operator.itemgetter(1), reverse=False)

        master_list = ['TToday',
                       'TWeek',
                       'Old90',
                       'Old365',
                       'Old1000',
                       'RT5',
                       'Followersx1',
                       'Followersx5',
                       'Followersx10',
                       'Followers/5',
                       'Followers/10',
                       'FollowBacker',
                       'FollowBackerPlus',
                       'ShortName15',
                       'CrypticName4',
                       'CatB',
                       'CatC',
                       'CatR']

        file = 'log/bots_%s.csv' % datetime.datetime.now().strftime('%Y_%m_%d')
        with open(file, "a") as my_file:
            my_file.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S\n'))
            my_file.write('Score,Screen Name,Followers,Friends,' + ','.join(master_list) + '\n')
            for x, y, z in sorted_x:
                followers = ''
                friends = ''
                if rt_dict[x]['followers'] is not None:
                    followers = str(rt_dict[x]['followers'])
                if rt_dict[x]['friends'] is not None:
                    friends = str(rt_dict[x]['friends'])

                my_file.write(str(y) + ',' + x + ',' + followers + ',' + friends + ',' + ','.join(
                    [str(bool_to_int(metric in z)) for metric in master_list]) + '\n')
                self.db.set_bot_score(screen_name=x,
                                      bot_score=y)

    # def get_tweeter_details(self, screen_name):
    #     t = (screen_name,)
    #     c.execute('SELECT profile_image_url, name FROM dim_tweeter WHERE screen_name=?', t)
    #     row = c.fetchone()
    #     profile_image_url = None
    #     name = None
    #     if row != None:
    #         profile_image_url = row[0]
    #         name = row[1]
    #
    #     return (profile_image_url, name)

    # def get_suspects(self, query_date):
    #     # Get tweeters that RTed today
    #
    #     # cutoffdate = (datetime.datetime.strptime(query_date, '%Y-%m-%d') - timedelta(days=4)).strftime('%Y-%m-%d')
    #     rows = self.db.get_bot_suspects(query_date)
    #
    #     screen_names = list()
    #     for (sn,) in rows:
    #         screen_names.append(sn)
    #
    #     return screen_names

    def refresh_dim_tweeter(self, query_date):
        rows = self.db.get_tweet_history(query_date)
        logger.info(f'{len(rows)} refreshed for {query_date} 00:00:00')

        for (sn, tweet_history, tweet_id) in rows:
            if tweet_id is None:
                today = ' '
            else:
                today = 'X'
            if tweet_history is None:
                tw_hist = today
            else:
                tw_hist = today + tweet_history[:6]
            self.db.set_tweet_history(screen_name=sn,
                                      tweet_history=tw_hist,
                                      bot_date=self.bot_date)

        return

    def get_rt_variance(self, query_date, user_dict):
        # Gets details of tweeters who RTed today
        rows = self.db.get_rt_variance(query_date)

        for (sn, sn_count, count, friends, followers, created_at, tweet_date, category) in rows:
            user_dict[sn] = {'sn': sn_count, 'rt': count, 'friends': friends, 'followers': followers,
                             'created_at': created_at, 'tweet_date': tweet_date, 'category': category}

        return user_dict


def bool_to_int(a):
    if a:
        return 1
    return 0


logger = logging.getLogger('findbots')


def main():
    parser = argparse.ArgumentParser(description='Find bots based on activity on the given day.')
    parser.add_argument('date', metavar='yyyy-mm-dd',
                        help='the date to process')

    args = parser.parse_args()

    environment = defaults.get_environment()
    db = DB(environment, args.date)

    date_to = args.date

    _ = FindBots(db, date_to)

    db.disconnect()


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
