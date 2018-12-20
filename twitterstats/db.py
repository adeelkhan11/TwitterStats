import datetime
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import List
from twitterstats.dbutil import DBUtil
from twitterstats import secommon
# from twitterstats.singleton import Singleton
from twitterstats.secommon import now, today, chop
from twitterstats.urdu import urdu_to_english

logger = logging.getLogger(__name__)


@dataclass
class NameScore:
    status_count: int = 0
    total_score: int = 0


@dataclass
class Range:
    min_id: int
    max_id: int
    processed: bool = False


@dataclass
class Hashtag:
    name: str
    ranges: List[Range] = field(default_factory=list)
    name_scores: List[NameScore] = field(default_factory=list)
    _state: str = ''

    def __post_init__(self):
        self.name_scores.append(NameScore())
        self.state = 'AUTO_DEL'

    def get_average_score(self, sample_size):
        total_score = sum([ns.total_score for ns in self.name_scores[-sample_size:]])
        total_count = sum([ns.status_count for ns in self.name_scores[-sample_size:]])

        result = 0 if total_count == 0 else total_score / total_count
        return result

    def get_status_count(self, sample_size):
        return sum([ns.status_count for ns in self.name_scores[-sample_size:]])

    @property
    def state(self) -> str:
        if self._state == '':
            state = DB.db.get_trends().get(self.name, None)
            if state is None:
                state = 'AUTO_DEL'
                DB.db.set_trend(self.name, state)
                DB.db.get_trends()[self.name] = state
            self._state = state

        return self._state

    @state.setter
    def state(self, v: str):
        if v != self.state:
            self._state = v
            DB.db.set_trend(self.name, v)
            DB.db.get_trends()[self.name] = v


# @dataclass_json
@dataclass
class Tweet:
    id: int
    created_at: str
    screen_name: str
    text: str
    retweet_count: int
    category: str


@dataclass
class PublishRetweet(Tweet):
    score: float
    bot_data_availability: bool
    rank: int = None
    drafted_date: str = None
    account: str = None

    def publish_dict(self):
        return {'tweet_id': str(self.id), 'type': 'retweet', 'tweeter_type': self.category,
                'tweet_created_at': self.created_at,
                'tweet_retweet_count': self.retweet_count,
                'bot_score': '{}/{}'.format(self.score, self.bot_data_availability), 'rank': self.rank,
                'tweet_screen_name': self.screen_name, 'head': self.text, 'drafted_at': self.drafted_date,
                'account': self.account}


class DB(DBUtil):
    db = None

    def __init__(self, environment, date):
        DBUtil.__init__(self, environment)
        if DB.db is None:
            DB.db = self
        else:
            raise Exception("DB instantiated twice.")

        self.fact_status = dict()
        self.tag_history = list()
        self.date_skey_cache = dict()
        self.fact_status_retweet_count = dict()
        self._tag_discovery = dict()
        self.name_scores = None
        self.category_scores = None
        self.timezone_scores = None
        self.location_scores = None
        self.sn_categories = None

        self.min_date = None
        self.range_min = None
        self.range_max = None

        self._baseline_tweet_id = None

        self.date_skey_request = 0
        self.date_skey_dblookup = 0
        self.word_skey_request = 0
        self.word_skey_dblookup = 0
        self.tweeter_skey_request = 0
        self.tweeter_skey_dblookup = 0
        self.date_skey_cache = {}
        self.word_skey_cache = {}
        self.tweeter_skey_cache = {}

        self.rated_tweeters = {}

        self.connect(date)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.disconnect()

    def connect(self, date='2999-12-31'):
        database = self.se_database('se', date)
        dim_database = self.se_database('se_dimension', date)
        print('Databases: {},  {}'.format(database, dim_database))
        self.conn = sqlite3.connect(database)
        self.c = self.conn.cursor()
        self.c.execute('ATTACH DATABASE ? AS dim', (dim_database,))
        self.c.execute('SELECT min_date from db_baseline')
        self.min_date = self.c.fetchone()[0]
        if self.min_date > date:
            logger.critical("Could not find database for date %s", date)
            exit(1)
        # return self.c

    def fetchall(self, sql, t):
        self.c.execute(sql, t)
        return self.c.fetchall()

    def mark_tweeter_as_bot(self, screen_name):
        t = ('R', self.min_date, -2, screen_name)
        self.c.execute('UPDATE dim_tweeter set category = ?, category_date = ?, bot_score = ? WHERE screen_name = ?', t)

    def set_retweeted(self, tweets):
        for tweet in tweets:
            t = ('Y', tweet.id)
            self.c.execute('update fact_status set retweeted = ? where id = ?', t)
        self.commit()

    def get_top_tweets(self, start_date, end_date):
        t = (start_date, end_date, self.env.cutoff_a[0], 'A', self.env.cutoff_b[0], 'G', self.env.cutoff_default[0])
        sql = """select t.id, t.created_at, t.screen_name,
        t.text, t.retweet_count, dt.category, t.retweet_count + (bot.bot_factor * 2) as score, bot.bot_data_availability
        from (
        select count(*) tweet_count, sum(IfNull(bot_score, 4) - 4) as bot_factor,
        sum(case when bot_score is null then 0 else 1 end) bot_data_availability, max(t.retweet_count) retweet_count,
        retweet_id, t.retweet_screen_name, t.text
        from fact_status t join dim_tweeter dt on t.tweeter_skey = dt.tweeter_skey
        where t.retweet_id != 0
        group by t.retweet_id, t.retweet_screen_name, t.text
        ) bot
        join fact_status t on bot.retweet_id = t.id
        join dim_tweeter dt on dt.screen_name = t.screen_name
        where t.created_at >= ?
        and t.created_at <= ?
        and bot.tweet_count > 1
        and t.retweet_id = 0
        and t.retweeted is null
        and ((t.retweet_count >= ? and dt.category = ?) or (t.retweet_count >= ? and dt.category < ?)
        or t.retweet_count > ?)
        order by t.retweet_count + (bot.bot_factor * 2) desc LIMIT 1000
        ;
        """
        self.c.execute(sql, t)
        rows = self.c.fetchall()

        tweets = [PublishRetweet(*row) for row in rows]
        return tweets

    def get_next_batch_id(self):
        batch_id = 1
        self.c.execute('SELECT max(batch_id) max_id FROM fact_status')
        row = self.c.fetchone()
        if row is not None and row[0] is not None:
            batch_id = row[0] + 1

        logger.info('Batch {}'.format(batch_id))
        return batch_id

    def get_baseline_tweet_id(self):
        if self._baseline_tweet_id is None:
            self.c.execute('SELECT min_tweet_id FROM db_baseline')
            row = self.c.fetchone()
            self._baseline_tweet_id = row[0]
        return self._baseline_tweet_id

    def get_famous_screen_names(self):
        self.c.execute('select screen_name from dim_tweeter where category < ?', ('C',))
        rows = self.c.fetchall()
        return [row[0] for row in rows]

    def get_next_read_sequence(self):
        self.c.execute('select max(ifnull(read_sequence, 0)) from dim_tweeter')
        row = self.c.fetchone()
        return int(row[0]) + 1

    def set_read_sequence(self, screen_name, read_sequence):
        t = (read_sequence, screen_name)
        self.c.execute('update dim_tweeter set read_sequence = ? where screen_name = ?', t)

    def set_tweeter_error(self, screen_name, error):
        t = (error, screen_name)
        self.c.execute('update dim_tweeter set error = ? where screen_name = ?', t)

    def get_next_screen_names_for_category(self, category, limit):
        t = (category + '%', limit)
        self.c.execute(
            """select screen_name
            from dim_tweeter
            where category like ? and error is null order by ifnull(read_sequence, 0) limit ?""",
            t)
        return [row[0] for row in self.c.fetchall()]

    def delete_batch(self, batch_id):
        t = (batch_id,)
        self.c.execute('delete from fact_status where batch_id = ?', t)

    def write_tweets(self):
        counter = 0
        for key, item in self.fact_status.items():
            # mark_database_activity()
            self.c.execute("""
                insert into fact_status
                 (id, created_at, screen_name, text, tweeter_skey, retweet_count, in_reply_to_status_id, date_skey,
                  retweet_id, retweet_created_at, retweet_screen_name, batch_id, english_words)
                values (?,?,?,?,?,?,?,?,?,?,?,?,?)""", item)
            counter += 1
        logger.info("%i tweets written." % counter)
        self.fact_status = dict()

    def update_max_id(self, hashtag, _id, min_id):
        raise (Exception("Update max id was called."))
        # if _id > 0:
        #     self.tag_history.append([hashtag, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), _id, min_id])

    def get_list_max_id(self, list_name):
        self.c.execute('select max(max_id) from tag_history where tag = ?', (list_name,))
        row = self.c.fetchone()
        return row[0]

    def write_list_max_id(self, list_name, max_id, min_id):
        t = (list_name, now(), max_id, min_id)
        self.c.execute('insert into tag_history (tag, date, max_id, min_id) values (?,?,?,?)', t)

    def write_tag_history(self):
        counter = 0
        for trend in self.tag_history:
            for id_range in trend.ranges:
                # mark_database_activity()
                if id_range.max_id is not None and id_range.processed:
                    t = (trend.name, now(), id_range.max_id, id_range.min_id)
                    self.c.execute('insert into tag_history (tag, date, max_id, min_id) values (?,?,?,?)', t)
                    # print "tag_history: %25s,%20s,%20d,%20d" % item
                    counter += 1
        logger.info("%i tag histories written." % counter)
        self.tag_history = list()

    def get_name_score(self, name, screen_name, location='', timezone='', ignore_category=False):
        if self.name_scores is None:
            self.load_name_score_data()

        location = self.none_to_empty(location)
        timezone = self.none_to_empty(timezone)
        category = self.sn_categories[screen_name] if screen_name in self.sn_categories else ''

        name_score = self.calculate_text_score(name, self.name_scores)
        category_score = 0 if ignore_category else self.calculate_text_score(category, self.category_scores)
        location_score = self.calculate_text_score(location, self.location_scores)
        timezone_score = self.calculate_text_score(timezone, self.timezone_scores)

        logger.debug("Name:%3d   Category:%3d   Location:%3d   Timezone:%3d  %-20s %s %-15s %-15s" % (
            name_score, category_score, location_score, timezone_score, chop(name, 20), category, chop(location, 15),
            chop(timezone, 15)))

        return category_score + min(10, max(-5, name_score + location_score + timezone_score))

    def get_tag_ranges(self, tag, min_override):
        # tag is a word with preceding hash
        result = Hashtag(name=tag)

        t = (tag.lower(),)
        self.c.execute('SELECT min_id, max_id FROM tag_history WHERE tag=? order by max_id desc', t)
        row = self.c.fetchone()
        prev_min = None
        # print "Min Override: %20d,%20d,%20d" % (min_override, row[0], row[1])
        while row is not None and row[0] is not None:
            (min_id, max_id) = row
            if max_id >= min_override:
                if len(result.ranges) == 0:
                    result.ranges.append(Range(min_id=max_id, max_id=None))
                elif max_id < prev_min:
                    result.ranges.append(Range(min_id=max_id, max_id=prev_min))
            else:
                if len(result.ranges) == 0:
                    result.ranges.append(Range(min_id=min_override, max_id=None))
                elif max_id < prev_min and min_override < prev_min:
                    result.ranges.append(Range(min_id=min_override, max_id=prev_min))

            prev_min = min_id
            row = self.c.fetchone()

        if prev_min is None or prev_min > min_override:
            result.ranges.append(Range(min_id=min_override, max_id=prev_min))

        return result

    def get_tag_completeness(self, tag):
        if self.range_min is None:
            # Get the min and max id ranges for the day
            sql = """select b.min_date, min(id), max(id)
            from fact_status s join db_baseline b
            on substr(s.created_at, 1, 10) = b.min_date"""
            self.c.execute(sql)
            self.min_date, self.range_min, self.range_max = self.c.fetchone()

        hashtag = self.get_tag_ranges(tag, self.range_min)
        total_gap = 0
        # print tag
        for r in hashtag.ranges:
            min_id = r.min_id
            if min_id < self.range_max:
                max_id = r.max_id if r.max_id is not None else self.range_max
                total_gap += max_id - min_id

        # print total_gap, max_id, min_id
        return round(1.0 - (float(total_gap) / float(self.range_max - self.range_min)), 3)

    # def update_tweet_words(self, id, english_words):
    #     self.fact_status[id][12] = english_words

    def update_tweet_words(self, _id, english_words):
        self.fact_status[_id][12] = english_words

    def get_trends(self):
        if len(self._tag_discovery) == 0:
            self.c.execute('select tag, result from tag_discovery order by discovery_time')
            row = self.c.fetchone()
            while row is not None:
                self._tag_discovery[row[0].lower()] = row[1]
                row = self.c.fetchone()

            logger.info("Fetched trends: %s" % self._tag_discovery)

        return self._tag_discovery

    def get_trend_discovery_times(self):
        result = {}
        self.c.execute('select tag, max(discovery_time) from tag_discovery group by tag')
        row = self.c.fetchone()
        while row is not None:
            result[row[0].lower()] = row[1]
            row = self.c.fetchone()

        return result

    def set_trend(self, trend, result):
        t = (trend, now())
        self.c.execute('DELETE FROM tag_discovery WHERE tag = ? AND discovery_time = ?', t)
        t = (trend, result, now())
        self.c.execute('INSERT INTO tag_discovery (tag, result, discovery_time) VALUES (?, ?, ?)', t)

    def load_name_score_data(self):
        if self.name_scores is None:
            self.name_scores = secommon.read_csv_hash('metadata/name_score.csv', remove_zero=True)
            self.category_scores = secommon.read_csv_hash('metadata/category_score.csv', remove_zero=True)
            self.timezone_scores = secommon.read_csv_hash('metadata/timezone_score.csv', remove_zero=True)
            self.location_scores = secommon.read_csv_hash('metadata/location_score.csv', remove_zero=True)

            sql = """select screen_name, category
                from dim_tweeter
                where category is not null"""
            self.c.execute(sql)
            rows = self.c.fetchall()
            self.sn_categories = dict()
            for (screen_name, category) in rows:
                self.sn_categories[screen_name] = category

    def get_date_skey(self, date):
        if date not in self.date_skey_cache:
            # mark_database_activity()
            t = (date,)
            self.c.execute('select date_skey from dim_date where date = ?', t)
            row = self.c.fetchone()
            if row is None:
                self.c.execute('insert into dim_date (date) values (?)', t)
                self.c.execute('select date_skey from dim_date where date = ?', t)
                row = self.c.fetchone()
            self.date_skey_cache[date] = row[0]

        return self.date_skey_cache[date]

    def tweet_is_duplicate(self, id_, created_at, screen_name, text, tweeter_skey, retweet_count, in_reply_to_status_id,
                           date_skey, retweet_id, retweet_created_at, retweet_screen_name, batch_id):
        result = 1
        if id_ not in self.fact_status and id_ not in self.fact_status_retweet_count:
            t = (id_,)
            self.c.execute('select id, retweet_count from fact_status where id = ?', t)
            row = self.c.fetchone()
            if row is None:
                self.fact_status[id_] = [id_, created_at, screen_name, text, tweeter_skey, retweet_count,
                                         in_reply_to_status_id, date_skey, retweet_id, retweet_created_at,
                                         retweet_screen_name, batch_id, '']
                result = 0
            elif retweet_count != row[1]:
                self.fact_status_retweet_count[id_] = retweet_count
                t = (retweet_count, id_)
                self.c.execute('update fact_status set retweet_count = ? where id = ?', t)
            elif retweet_count == row[1]:
                self.fact_status_retweet_count[id_] = retweet_count
        elif id_ in self.fact_status_retweet_count and retweet_count != self.fact_status_retweet_count[id_]:
            self.fact_status_retweet_count[id_] = retweet_count
            t = (retweet_count, id_)
            self.c.execute('update fact_status set retweet_count = ? where id = ?', t)

        return result

    def get_tweeter_skey(self, screen_name, name='', followers_count=0, friends_count=0, lang='', time_zone='',
                         verified='', id_=0, statuses_count=0, profile_image_url='', created_at='', location=''):
        # mark_database_activity()
        snl = screen_name.lower()
        if snl not in self.tweeter_skey_cache or (self.tweeter_skey_cache[snl] < 0 and name != ''):
            # mark_database_activity()
            t = (snl,)
            self.c.execute(
                """select tweeter_skey, name, followers_count, lang, time_zone,
                verified, statuses_count, category, profile_image_url,
                friends_count, created_at, location
                from dim_tweeter
                where screen_name_lower = ?""",
                t)
            row = self.c.fetchone()
            if row is None:
                if name == '':
                    t = (snl, screen_name)
                    self.c.execute('insert into dim_tweeter (screen_name_lower, screen_name) values (?,?)', t)
                else:
                    t = (snl, screen_name, name, followers_count, lang, time_zone, verified, statuses_count,
                         profile_image_url, friends_count, created_at, location)
                    self.c.execute(
                        """insert into dim_tweeter (screen_name_lower, screen_name, name
                        , followers_count, lang, time_zone, verified, statuses_count
                        , profile_image_url, friends_count, created_at, location)
                        values (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        t)
                t = (snl,)
                self.c.execute('select tweeter_skey from dim_tweeter where screen_name_lower = ?', t)
                row = self.c.fetchone()
            elif name != '' and (
                    row[1] != name or row[2] != followers_count or row[3] != lang or row[4] != time_zone or
                    row[5] != verified or row[6] != statuses_count or row[7] != friends_count or
                    row[8] != created_at or row[9] != location):  # keep name up to date
                t = (
                    screen_name, name, followers_count, lang, time_zone, verified, id_, statuses_count,
                    profile_image_url,
                    friends_count, created_at, location, snl)
                self.c.execute(
                    """update dim_tweeter
                    set screen_name = ?, name = ?, followers_count = ?
                    , lang = ?, time_zone = ?, verified = ?, id = ?
                    , statuses_count = ?, profile_image_url = ?
                    , friends_count = ?, created_at = ?, location = ?
                    where screen_name_lower = ?""",
                    t)

            self.tweeter_skey_dblookup += 1
            # X: Banned
            # R: Robot - confirmed
            if len(row) > 7 and row[7] is not None and row[7] >= 'X':
                self.tweeter_skey_cache[snl] = 0
            else:
                if name != '':
                    self.tweeter_skey_cache[snl] = row[0]
                else:
                    self.tweeter_skey_cache[snl] = row[0] * -1
            if len(row) > 7 and row[7] is not None and row[7] < 'C':
                self.rated_tweeters[snl] = row[7]

        self.tweeter_skey_request += 1
        return abs(self.tweeter_skey_cache[snl])

    def write_hashtag_word(self, tweetdate, hashtag, word, count):
        # mark_database_activity()
        date_skey = self.get_date_skey(tweetdate)
        (word_skey, english_word) = self.get_word_skey(word, tweetdate)
        (tag_skey, english_tag) = self.get_word_skey(hashtag, tweetdate)
        t = (word_skey, tag_skey, date_skey)
        self.c.execute(
            'select count from fact_daily_hashtag_word where word_skey = ? and tag_skey = ? and date_skey = ?', t)
        row = self.c.fetchone()
        if row is None:
            t = (word_skey, tag_skey, date_skey, count)
            self.c.execute(
                'insert into fact_daily_hashtag_word (word_skey, tag_skey, date_skey, count) values (?,?,?,?)', t)
        else:
            total_count = row[0] + count
            t = (total_count, word_skey, tag_skey, date_skey)
            self.c.execute(
                'update fact_daily_hashtag_word set count = ? where word_skey = ? and tag_skey = ? and date_skey = ?',
                t)

    def get_rt_variance(self, query_date):
        t = (query_date + ' 00:00:00', query_date + ' 23:59:59')
        self.c.execute(
            """select s.screen_name, count(distinct retweet_screen_name), count(*), t.friends_count,
            t.followers_count, t.created_at, t.tweet_date, t.category
            from fact_status s
            join dim_tweeter t on t.tweeter_skey = s.tweeter_skey
            where retweet_id != 0
            and s.created_at between ? and ?
            group by s.screen_name, t.friends_count, t.followers_count, t.created_at, t.tweet_date, t.category""",
            t)
        return self.c.fetchall()

    # def get_bot_suspects(self, query_date):
    #     t = (query_date + ' 00:00:00', query_date + ' 23:59:59')
    #     self.c.execute(
    #         """select distinct s.screen_name
    #         from fact_status s
    #         join dim_tweeter t on t.tweeter_skey = s.tweeter_skey
    #         where created_at between ? and ?""",
    #         t)
    #     return self.c.fetchall()

    def get_tweet_history(self, query_date):
        t = (query_date + ' 00:00:00', query_date + ' 23:59:59')
        self.c.execute(
            """select t.screen_name, t.tweet_date, max(s.id)
            from dim_tweeter t
            left outer join fact_status s on s.tweeter_skey = t.tweeter_skey and s.retweet_id = 0
            and s.created_at between ? and ?
            where (TRIM(IFNULL(t.tweet_date, '')) != ''
            or s.id is not null)
            group by t.screen_name, t.tweet_date""",
            t)
        return self.c.fetchall()

    def set_tweet_history(self, screen_name, tweet_history, bot_date):
        t = (bot_date, tweet_history, screen_name.lower())
        self.c.execute('update dim_tweeter set bot_date = ?, tweet_date = ? where screen_name_lower = ?', t)

    def set_bot_score(self, screen_name, bot_score):
        t = (today(), bot_score, screen_name.lower())
        self.c.execute('update dim_tweeter set bot_date = ?, bot_score = ? where screen_name_lower = ?', t)

    def write_tweeter_word(self, tweetdate, tweeter, word, count):
        # mark_database_activity()
        date_skey = self.get_date_skey(tweetdate)
        (word_skey, english_word) = self.get_word_skey(word, tweetdate)
        tweeter_skey = self.get_tweeter_skey(tweeter)
        t = (word_skey, tweeter_skey, date_skey)
        self.c.execute(
            'select count from fact_daily_tweeter_word where word_skey = ? and tweeter_skey = ? and date_skey = ?', t)
        row = self.c.fetchone()
        if row is None:
            t = (word_skey, tweeter_skey, date_skey, count)
            self.c.execute(
                'insert into fact_daily_tweeter_word (word_skey, tweeter_skey, date_skey, count) values (?,?,?,?)', t)
        else:
            total_count = row[0] + count
            t = (total_count, word_skey, tweeter_skey, date_skey)
            self.c.execute(
                """update fact_daily_tweeter_word
                set count = ?
                where word_skey = ? and tweeter_skey = ? and date_skey = ?""",
                t)

    def write_tweeter_mention(self, tweetdate, tweeter, mention, count):
        # mark_database_activity()
        date_skey = self.get_date_skey(tweetdate)
        tweeter_skey = self.get_tweeter_skey(tweeter)
        mention_skey = self.get_tweeter_skey(mention)
        t = (mention_skey, tweeter_skey, date_skey)
        self.c.execute(
            """select count
            from fact_daily_tweeter_mention
            where mentioned_tweeter_skey = ? and tweeter_skey = ? and date_skey = ?""",
            t)
        row = self.c.fetchone()
        if row is None:
            t = (mention_skey, tweeter_skey, date_skey, count)
            self.c.execute(
                """insert into fact_daily_tweeter_mention (mentioned_tweeter_skey, tweeter_skey, date_skey, count)
                values (?,?,?,?)""",
                t)
        else:
            total_count = row[0] + count
            t = (total_count, mention_skey, tweeter_skey, date_skey)
            self.c.execute(
                """update fact_daily_tweeter_mention
                set count = ?
                where mentioned_tweeter_skey = ? and tweeter_skey = ? and date_skey = ?""",
                t)

    def write_tag(self, tweetdate, tag, count):
        # mark_database_activity()
        date_skey = self.get_date_skey(tweetdate)
        (word_skey, english_word) = self.get_word_skey(tag.lower(), tweetdate)
        t = (word_skey, date_skey, tag)
        self.c.execute('select count from fact_daily_hashtag where word_skey = ? and date_skey = ? and hashtag = ?', t)
        row = self.c.fetchone()
        if row is None:
            t = (word_skey, date_skey, tag, count)
            try:
                self.c.execute('insert into fact_daily_hashtag (word_skey, date_skey, hashtag, count) values (?,?,?,?)',
                               t)
            except sqlite3.IntegrityError:
                print("Could not save fact_daily_hashtag:", word_skey, date_skey, tag, count)
                raise
        else:
            total_count = row[0] + count
            t = (total_count, word_skey, date_skey, tag)
            self.c.execute(
                'update fact_daily_hashtag set count = ? where word_skey = ? and date_skey = ? and hashtag = ?', t)

    def write_tag_tweeter(self, tweetdate, tag, tweeter, count):
        # mark_database_activity()
        date_skey = self.get_date_skey(tweetdate)
        (tag_skey, english_tag) = self.get_word_skey(tag.lower(), tweetdate)
        tweeter_skey = self.get_tweeter_skey(tweeter)
        t = (tag_skey, tweeter_skey, date_skey)
        self.c.execute(
            'select count from fact_daily_hashtag_tweeter where tag_skey = ? and tweeter_skey = ? and date_skey = ?', t)
        row = self.c.fetchone()
        if row is None:
            t = (tag_skey, tweeter_skey, date_skey, count)
            self.c.execute(
                'insert into fact_daily_hashtag_tweeter (tag_skey, tweeter_skey, date_skey, count) values (?,?,?,?)', t)
        else:
            total_count = row[0] + count
            t = (total_count, tag_skey, tweeter_skey, date_skey)
            self.c.execute(
                """update fact_daily_hashtag_tweeter
                set count = ?
                where tag_skey = ? and tweeter_skey = ? and date_skey = ?""",
                t)

    def write_tag_tag(self, tweetdate, tag, other_tag, count):
        # mark_database_activity()
        date_skey = self.get_date_skey(tweetdate)
        (tag_skey, english_tag) = self.get_word_skey(tag.lower(), tweetdate)
        (other_tag_skey, english_other_tag) = self.get_word_skey(other_tag.lower(), tweetdate)
        t = (other_tag_skey, tag_skey, date_skey)
        self.c.execute(
            'select count from fact_daily_hashtag_hashtag where other_tag_skey = ? and tag_skey = ? and date_skey = ?',
            t)
        row = self.c.fetchone()
        if row is None:
            t = (other_tag_skey, tag_skey, date_skey, count)
            self.c.execute(
                'insert into fact_daily_hashtag_hashtag (other_tag_skey, tag_skey, date_skey, count) values (?,?,?,?)',
                t)
        else:
            total_count = row[0] + count
            t = (total_count, other_tag_skey, tag_skey, date_skey)
            self.c.execute(
                """update fact_daily_hashtag_hashtag
                set count = ?
                where other_tag_skey = ? and tag_skey = ? and date_skey = ?""",
                t)

    def write_tag_score(self, tag, tweet_count, score, max_id):
        t = (tag, tweet_count, score, max_id, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        self.c.execute('insert into tag_score (tag, tweet_count, score, max_id, score_time) values (?,?,?,?,?)', t)

    def get_word_skey(self, word, date):
        # mark_database_activity()
        if word not in self.word_skey_cache:
            # mark_database_activity()
            t = (word,)
            self.c.execute('select word_skey, english_word from dim_word where word = ?', t)
            row = self.c.fetchone()
            if row is None:
                englishword = urdu_to_english(word)
                t = (word, englishword, date)
                self.c.execute('insert into dim_word (word, english_word, created_date) values (?,?,?)', t)
                t = (word,)
                self.c.execute('select word_skey, english_word from dim_word where word = ?', t)
                row = self.c.fetchone()
            # if the English word isn't stored in the db, then store it
            if row[1] is None:
                englishword = urdu_to_english(word)
                t = (englishword, word)
                self.c.execute('update dim_word set english_word = ? where word = ?', t)
                row = (row[0], englishword)
            self.word_skey_dblookup += 1
            self.word_skey_cache[word] = row

        self.word_skey_request += 1
        return self.word_skey_cache[word]

    def write_daily_followers(self, tweeter_skey, date_skey, followers_count):
        # mark_database_activity()
        t = (tweeter_skey, date_skey)
        self.c.execute('select followers_count from fact_daily_followers where tweeter_skey = ? and date_skey = ?', t)
        row = self.c.fetchone()
        if row is None:
            t = (tweeter_skey, date_skey, followers_count)
            self.c.execute('insert into fact_daily_followers (tweeter_skey, date_skey, followers_count) values (?,?,?)',
                           t)
        elif followers_count != row[0]:
            t = (followers_count, tweeter_skey, date_skey)
            self.c.execute(
                'update fact_daily_followers set followers_count = ? where tweeter_skey = ? and date_skey = ?', t)

    def get_list_additions(self):
        sql = """select screen_name, category, list
        from dim_tweeter
        where category in ('A', 'B', 'C', 'D')
        and (list is null or (list not like category || '%' and list not like 'ERROR%'))
        order by category, category_date
        LIMIT 50"""
        self.c.execute(sql)
        return self.c.fetchall()

    def get_list_removals(self):
        sql = """select screen_name, category, list
        from dim_tweeter
        where (category is null or category not in ('A', 'B', 'C', 'D'))
        and list is not null
        order by category, category_date
        LIMIT 50"""
        self.c.execute(sql)
        return self.c.fetchall()

    def add_to_list(self, screen_name, list_name):
        sql = """update dim_tweeter
        set list = ?
        where screen_name = ?"""
        t = (list_name, screen_name)
        self.c.execute(sql, t)

    def remove_from_list(self, screen_name):
        self.add_to_list(screen_name, list_name=None)

    def set_env_list_count(self, list_name, count):
        try:
            for l in self.env.lists[list_name[0]]:
                if l['name'] == list_name:
                    l['count'] = count
        except TypeError:
            logger.error(f'Setting list {list_name} count to {count}')
            logger.error(self.env.lists)
            raise

    def load_list_levels(self):
        sql = """select list, count(*)
        from dim_tweeter
        where list is not null
        and list not like 'ERROR%'
        group by list"""

        self.c.execute(sql)
        for list_name, count in self.c.fetchall():
            self.set_env_list_count(list_name, count)

    def get_tweeter_promotion_stats(self):
        sql = """select t.screen_name, sum(case when ifnull(w.relevance, 'bob') = 'positive' then 1 else 0 end) pos
        , sum(case when ifnull(w.relevance, 'bob') = 'negative' then 1 else 0 end) neg
        , sum(case when ifnull(w.generic, 'bob') = 'B' then 1 else 0 end) blocked
        , t.category, t.relevance_score, t.followers_count, t.name, t.location, t.time_zone
        from dim_word w 
        join fact_daily_hashtag_tweeter ht
         on w.word_skey = ht.tag_skey
         join dim_tweeter t on t.tweeter_skey = ht.tweeter_skey 
         where ifnull(t.category, 'Z') >= 'C' 
        and ifnull(t.category, 'A') < 'G' 
        and (w.relevance is not null or w.generic is not null)
        group by t.screen_name 
        having count(*) >= 5"""

        self.c.execute(sql)
        return self.c.fetchall()

    def get_tweeter_relevance_score(self, screen_name):
        sql = """SELECT name, category, relevance_score, location, time_zone, followers_count
            FROM dim_tweeter
            WHERE screen_name = ?"""
        t = (screen_name, )
        self.c.execute(sql, t)
        result = self.c.fetchone()
        return result

    def get_tweeter_category_counts(self):
        sql = """SELECT CATEGORY, COUNT(*)
                FROM DIM_TWEETER
                GROUP BY CATEGORY;"""
        self.c.execute(sql)
        return self.c.fetchall()

    def get_tweeter_category_tweet_counts(self, date_skey):
        sql = """SELECT t.category, count(s.id)
                FROM FACT_STATUS s
                JOIN DIM_TWEETER t
                on s.tweeter_skey = t.tweeter_skey
                WHERE date_skey = ?
                GROUP BY t.category"""
        self.c.execute(sql, (date_skey,))
        return self.c.fetchall()

    def get_retweet_received_counts_for_trend(self, date, english_trend):
        tweeps = dict()
        sql = """select screen_name, sum(retweet_count), sum(botness) from (
            select t.screen_name, t.id, t.retweet_count, SUM((IfNull(dt.bot_score, 4) - 4) / 2) botness
            from fact_status t
            join fact_status t2 on t.id = t2.retweet_id
            join dim_tweeter dt on t2.tweeter_skey = dt.tweeter_skey
            join dim_tweeter dt1 on t.tweeter_skey = dt1.tweeter_skey
            where t.retweet_id = 0
            and (t.english_words like ?)
            and t.created_at like ?
            and ifnull(dt1.category, 'G') < 'V'
            group by t.screen_name, t.id, t.retweet_count
            )
            group by screen_name
            order by sum(retweet_count) desc limit 500;
            """
        t = ("%~" + english_trend + "~%", date + "%")
        self.c.execute(sql, t)
        rows = self.c.fetchall()
        for row in rows:
            tweeps[row[0]] = {'rt_received_count': int(row[1]), 'botness': int(row[2])}

        t = ('tweet_count', 'rt_count', "%~" + english_trend + "~%", date + "%", 'tweet_count', 'rt_count')
        self.c.execute(
            """select screen_name, case when retweet_id = 0 then ? else ? end as ttype, count(*)
            from fact_status t
            where (english_words like ?)
            and t.created_at like ?
            group by screen_name, case when retweet_id = 0 then ? else ? end order by count(*) desc limit 1000""",
            t)

        rows = self.db.c.fetchall()
        for row in rows:
            if row[0] in tweeps:
                tweeps[row[0]][row[1]] = int(row[2])
            else:
                tweeps[row[0]] = {row[1]: int(row[2])}

        return tweeps

    def get_top_mentions_for_date(self, date):
        sql = """select screen_name, SUM((IfNull(bot_score, 4) - 4) / 2) from
            (select distinct dt.date, f.mentioned_tweeter_skey, f.tweeter_skey, d.screen_name, d2.bot_score
            from fact_daily_tweeter_mention f
            join dim_tweeter d on f.mentioned_tweeter_skey = d.tweeter_skey
            join dim_tweeter d2 on f.tweeter_skey = d2.tweeter_skey
            join dim_date dt on f.date_skey = dt.date_skey
            where f.mentioned_tweeter_skey != f.tweeter_skey
            and dt.date = ?
            )
            group by screen_name having count(*) > 30 order by SUM((IfNull(bot_score, 4) - 4) / 2) desc
            ;"""
        t = (date,)
        self.c.execute(sql, t)
        return self.c.fetchall()

    def get_tags_from_tag_history(self, date):
        t = (date, '#%')
        self.db.c.execute('select distinct tag from tag_history where date >= ? and tag like ?', t)
        rows = self.c.fetchall()

        return frozenset([row[0].lower()[1:] for row in rows])

    def get_trend_groups(self, date_skey):
        sql = """
            select * from
            (select w1.english_word, w2.english_word 
            from fact_daily_hashtag_hashtag hh 
            join dim_word w1 on w1.word_skey = hh.tag_skey 
            join dim_word w2 on w2.word_skey = hh.other_tag_skey 
            join (select word_skey, sum(count) cnt
                from fact_daily_hashtag 
                where date_skey = ? 
                group by word_skey 
                having sum(count) > 30) daily on daily.word_skey = hh.tag_skey 
            where hh.date_skey = ? 
            group by w1.english_word, w2.english_word 
            having sum(count) > daily.cnt / 4 
            intersect
            select w2.english_word, w1.english_word 
            from fact_daily_hashtag_hashtag hh 
            join dim_word w1 on w1.word_skey = hh.tag_skey 
            join dim_word w2 on w2.word_skey = hh.other_tag_skey 
            join (select word_skey, sum(count) cnt 
                from fact_daily_hashtag 
                where date_skey = ? 
                group by word_skey 
                having sum(count) > 30) daily on daily.word_skey = hh.tag_skey 
            where hh.date_skey = ? 
            group by w1.english_word, w2.english_word 
            having sum(count) > daily.cnt / 4)"""
        t = (date_skey, date_skey, date_skey, date_skey)
        self.c.execute(sql, t)
        return self.c.fetchall()

    def get_tag_botness(self, date_skey):
        sql = """select w.word, count(*), SUM((IfNull(t.bot_score, 3) - 3) / 3) botness
            from fact_daily_hashtag_tweeter ht
            join dim_word w on ht.tag_skey = w.word_skey
            join dim_tweeter t on ht.tweeter_skey = t.tweeter_skey
            where ht.date_skey = ?
            group by w.word having count(*) >= 5
            order by count(*)"""

        t = (date_skey,)
        self.c.execute(sql, t)
        return self.c.fetchall()

    def get_tag_tweet_counts(self, date_skey):
        t = (date_skey,)
        self.c.execute(
            """select dh.hashtag, dh.count
            from fact_daily_hashtag dh
            join dim_word w on w.word_skey = dh.word_skey
            where w.generic is null and dh.date_skey = ? and dh.count > 5
            order by dh.count""",
            t)
        return self.c.fetchall()

    def get_tweeter_followers_count(self, screen_name):
        sql = """SELECT followers_count
                FROM DIM_TWEETER
                WHERE screen_name_lower = ?"""
        self.c.execute(sql, (screen_name,))
        return self.c.fetchone()[0]

    def get_trends_relevance(self):
        sql = '''select dh.hashtag, w.relevance, sum(dh.count)
            from fact_daily_hashtag dh
            join dim_word w on w.word_skey = dh.word_skey
            join dim_date d on d.date_skey = dh.date_skey
            where w.generic is null -- and d.date >= ?
            group by dh.hashtag having sum(dh.count) > 10
        '''
        # t = (yesterday, )
        self.c.execute(sql)
        return self.c.fetchall()

    def get_related_trends(self):
        sql = '''SELECT IFNULL(w1.hashtag, w1.word), IFNULL(w2.hashtag, w2.word), sum(count)
        FROM fact_daily_hashtag_hashtag dhh
        JOIN dim_word w1
          ON w1.word_skey = dhh.tag_skey
        JOIN dim_word w2
          ON w2.word_skey = dhh.other_tag_skey
        group by IFNULL(w1.hashtag, w1.word), IFNULL(w2.hashtag, w2.word)
        order by IFNULL(w1.hashtag, w1.word), sum(count) desc
        '''

        self.c.execute(sql)
        return self.c.fetchall()

    def get_top_tag_scores(self):
        sql = '''SELECT tag, tweet_count, score
        FROM tag_score
        order by score_time desc
        '''

        self.c.execute(sql)
        return self.c.fetchall()

    def get_relevant_words(self):
        relevant_words = dict()
        self.c.execute('SELECT word, relevance FROM dim_word where relevance is not null')
        rows = self.c.fetchall()
        for word, relevance in rows:
            relevant_words[word] = relevance
        return relevant_words

    def get_generic_words(self):
        generic_words = dict()
        self.c.execute('SELECT word, generic FROM dim_word where generic is not null')
        rows = self.c.fetchall()
        for word, generic in rows:
            generic_words[word] = generic
        return generic_words

    def get_tag_discovery_result(self, tag):
        t = (tag,)
        self.c.execute('SELECT result FROM tag_discovery where lower(tag) = ? order by discovery_time desc limit 1',
                       t)
        row = self.c.fetchone()
        return row[0] if row is not None else None

    def get_top_hashtags(self, start_date, end_date):
        # This should be changed to a single day.
        t = (start_date, end_date)
        self.c.execute(
            """select dh.hashtag, sum(dh.count)
            from fact_daily_hashtag dh
            join dim_word w on w.word_skey = dh.word_skey
            join dim_date d on d.date_skey = dh.date_skey
            where w.generic is null
            and d.date >= ?
            and d.date < ?
            group by dh.hashtag
            having sum(dh.count) > 30
            order by sum(dh.count)""",
            t)

        return self.c.fetchall()

    def set_word_hashtag(self, word, hashtag):
        t = (hashtag, word)
        self.c.execute('update dim_word set hashtag = ? where word = ?', t)

    def save_tag_discovery(self, tag, result):
        t = (tag, result, now())
        self.c.execute('INSERT INTO tag_discovery (tag, result, discovery_time) VALUES (?, ?, ?)', t)

    def set_word_relevance(self, word, relevance):
        self.c.execute('UPDATE dim_word SET relevance = ? where word = ?',
                       (relevance, word.lower()))

    def set_word_generic(self, word, generic):
        self.c.execute('UPDATE dim_word SET generic = ? where word = ?',
                       (generic, word.lower()))

    def set_tweeter_category(self, screen_name, category, relevance_score=None):
        if relevance_score is None:
            t = (category, today(), screen_name.lower())
            self.c.execute('UPDATE dim_tweeter set category = ?, category_date = ? WHERE screen_name_lower = ?', t)
        else:
            t = (category, today(), relevance_score, screen_name)
            self.c.execute(
                "update dim_tweeter set category = ?, category_date = ?, relevance_score = ? WHERE screen_name = ?",
                t)

    def set_tweeter_category_by_date(self, date_category_was_set, current_category, new_category):
        t = (new_category, today(), current_category, date_category_was_set)
        self.c.execute(
            "update dim_tweeter set category = ?, category_date = ? WHERE category = ? AND category_date < ?",
            t)
        logger.info('{} records demoted from {} to {}.'.format(self.c.rowcount, current_category, new_category))
