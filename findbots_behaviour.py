import logging

import defaults
from datetime import timedelta
import datetime
import os.path

from twitterstats.db import DB
from twitterstats.secommon import today


class FindBotsBehaviour:
    def __init__(self, environment, db):
        self.bots = dict()
        self.environment = environment
        self.db = db

        bot_file = f'{self.environment.bot_data_directory}/parallel_tweets_%s.txt'
        sql = '''
        select group_concat(s.screen_name, ',')
        from fact_status s
        join dim_tweeter t on t.screen_name = s.screen_name
        where ifnull(t.category, 'E') not in ('A', 'B', 'R', 'F', 'X', 'Y', 'Z')
        and s.created_at like ?
        group by substr(s.created_at, 1, 19), retweet_screen_name, text
        having count(*) >= 5
        order by count(*) desc
        limit 1000;
        '''
        self.find_bots(bot_file, sql, 5, 20)

        # rt per UserWarning
        bot_file = f'{self.environment.bot_data_directory}/rt_per_user_%s.txt'
        sql = '''
        select screen_name, name, category, bot_score, rtnames, rt_count, distinct_rt,
        rt_count / distinct_rt, t_count from (
        select t.screen_name, t.name, t.category, t.bot_score, group_concat(s.retweet_screen_name, ',') as rtnames,
        sum(case when s.retweet_id = 0 then 0 else 1 end) as rt_count
        , sum(case when s.retweet_id = 0 then 1 else 0 end) as t_count,
        count(distinct retweet_screen_name) as distinct_rt
        from dim_tweeter t join fact_status s on t.tweeter_skey = s.tweeter_skey
        where ifnull(category, 'E') not in ('A', 'B', 'R', 'F', 'X', 'Y', 'Z')
        and s.created_at like ?
        group by t.screen_name, t.name, t.category, t.bot_score
        having count(*) > 20
        and count(distinct retweet_screen_name) < 10
        )
        where (rt_count / distinct_rt > 5 and t_count = 0 and rt_count > 30)
        or (rt_count / distinct_rt > 8 and t_count < 3 and rt_count > 20)
        '''
        self.find_bots(bot_file, sql, 5, 2)

        self.mark_bots(self.bots.keys())

    def find_bots(self, my_file, sql, days, bot_threshold):
        bot_file = my_file % self.db.min_date
        logger.info("Generating %s" % bot_file)
        t = (self.db.min_date + '%',)
        rows = self.db.fetchall(sql, t)
        with open(bot_file, 'w') as bfile:
            for row in rows:
                bfile.write(row[0] + "\n")
        self.count_words(my_file, days, bot_threshold)

    def count_words(self, my_file, days, bot_threshold):
        # global bots
        words = dict()
        for i in range(days):
            file_date = (datetime.datetime.strptime(self.db.min_date, "%Y-%m-%d") -
                         timedelta(days=i)).strftime('%Y-%m-%d')
            bot_file = my_file % file_date
            if os.path.isfile(bot_file):
                with open(bot_file) as f:
                    for full_line in f:
                        line = full_line.rstrip('\n')
                        parts = line.split(',')
                        for part in parts:
                            words[part] = words[part] + 1 if part in words else 1

        todays_bot_list = self.unique_words(my_file, 1)

        for k, v in words.items():
            # print("%3d %s" % (v, k))
            if v >= bot_threshold and k in todays_bot_list:
                self.bots[k] = 1

    def unique_words(self, my_file, days):
        # global bots
        words = dict()
        for i in range(days):
            file_date = (datetime.datetime.strptime(self.db.min_date, "%Y-%m-%d") -
                         timedelta(days=i)).strftime('%Y-%m-%d')
            bot_file = my_file % file_date
            if os.path.isfile(bot_file):
                with open(bot_file) as f:
                    for full_line in f:
                        line = full_line.rstrip('\n')
                        parts = line.split(',')
                        for part in parts:
                            words[part] = 1

        return words.keys()

    def mark_bots(self, bot_list):
        bot_file = f'{self.environment.bot_data_directory}/bots_marked_{self.db.min_date}.txt'
        with open(bot_file, 'w') as bfile:
            for screen_name in bot_list:
                self.db.mark_tweeter_as_bot(screen_name)
                bfile.write("%s\n" % screen_name)
                logger.info("Category for %s changed to R (marked as bot)" % screen_name)


logger = logging.getLogger('findbots_behaviour')


def main():
    environment = defaults.get_environment()
    db = DB(environment, today())

    _ = FindBotsBehaviour(environment, db)

    db.disconnect()


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
