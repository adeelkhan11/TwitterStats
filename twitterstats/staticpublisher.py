import json
import urllib.request
import urllib.parse
from dataclasses import is_dataclass
from datetime import datetime

from jinja2 import Environment, FileSystemLoader
import os
from shutil import copyfile

from twitterstats.publishdb import Trend, Mention, Tweep
from .publishdb import get_session
from twitterstats.urdu import unicode_to_key
# from markdown2 import markdown

WEB_ROOT = 'website'


class StaticPublisher:
    def __init__(self, date: datetime):
        self.trends_tweet = None
        self.mentions_tweet = None
        self.date = date
        self.session = get_session(date)
        self.date_str = self.date.strftime('%Y-%m-%d')
        self.trend_pages = dict()
        self.env = Environment(loader=FileSystemLoader('%s/../templates/' % os.path.dirname(__file__)))

    def publish(self, tweets):
        for t in tweets:
            if t.type == 'trends':
                print('Trendy!')
            else:
                print(f'Tweet type: {t.type}')

        template = self.env.get_template('trenders.html')
        copyfile('templates/grid.css', f'{WEB_ROOT}/grid.css')
        for t in tweets:
            if t.type == 'trends':
                self.trends_tweet = t
            elif t.type == 'mentions':
                self.mentions_tweet = t
            elif t.type == 'trenders':
                items = self.sorted_subitems(t)
                delete_q = Tweep.__table__.delete().where(Tweep.date == self.date).where(Tweep.trend == t.trend)
                self.session.execute(delete_q)
                self.session.commit()
                for me in items:
                    tweep = Tweep(date=self.date,
                                  trend=t.trend,
                                  screen_name=me.tweet_text,
                                  name=me.display_text,
                                  image=me.display_image,
                                  score=me.score,
                                  tweets_posted=me.tweet_count,
                                  rts_posted=me.rt_count,
                                  rts_received=me.rt_received_count,
                                  botness=me.botness)
                    self.session.add(tweep)
                self.session.commit()
                html_content = template.render(title=t.image_head, items=items)
                file_name = f'/{self.date_str}/trends/{unicode_to_key(t.trend)}.html'
                self.trend_pages[t.trend] = file_name
                os.makedirs(os.path.dirname(WEB_ROOT + file_name), exist_ok=True)
                with open(WEB_ROOT + file_name, 'w') as file:
                    file.write(html_content)

        # exit()
        return

        url = env.base_url + 'deliver'
        account = env.default_account

        payload = {}
        for k, v in data.items():
            if isinstance(v, list):
                value = list()
                for val in v:
                    value.append(val.publish_dict() if is_dataclass(val) else val)
            elif is_dataclass(v):
                value = v.publish_dict()
            else:
                value = v
            payload[k] = value
        print(payload)

        return
        payload_json = json.dumps(payload)
        values = {'data': payload_json,
                  'type': type_,
                  'account': account}
        print('StaticPublisher:')
        print(values)
        data = urllib.parse.urlencode(values).encode('utf-8')
        response = urllib.request.urlopen(url, data, timeout=60)
        the_page = response.read()
        print(the_page)

    def commit(self):
        # session = get_session(self.date)
        # env = Environment(loader=FileSystemLoader('%s/../templates/' % os.path.dirname(__file__)))
        trends_template = self.env.get_template('trends.html')
        mentions_template = self.env.get_template('mentions.html')
        main_template = self.env.get_template('main.html')
        copyfile('templates/grid.css', f'{WEB_ROOT}/grid.css')
        trends, mentions = None, None
        trends_title, mentions_title = None, None

        t = self.trends_tweet
        trends = self.sorted_subitems(t)
        delete_q = Trend.__table__.delete().where(Trend.date == self.date)
        self.session.execute(delete_q)
        self.session.commit()
        for tr in trends:
            tr.page_url = self.trend_pages.get(tr.tweet_text[1:])
            trend = Trend(date=self.date,
                          trend=tr.tweet_text,
                          score=tr.score,
                          tweet_count=tr.tweet_count,
                          tweep_count=tr.tweep_count)
            self.session.add(trend)
        self.session.commit()
        trends_title = t.image_head
        trends_html_content = trends_template.render(title=t.image_head, items=trends)
        file_name = f'{WEB_ROOT}/{self.date_str}/trends.html'
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, 'w') as file:
            file.write(trends_html_content)

        if self.mentions_tweet is not None:
            t = self.mentions_tweet
            mentions = self.sorted_subitems(t)
            delete_q = Mention.__table__.delete().where(Mention.date == self.date)
            self.session.execute(delete_q)
            self.session.commit()
            for me in mentions:
                mention = Mention(date=self.date,
                                  screen_name=me.tweet_text,
                                  name=me.display_text,
                                  image=me.display_image,
                                  score=me.score)
                self.session.add(mention)
            self.session.commit()
            mentions_title = t.image_head
            mentions_html_content = mentions_template.render(title=t.image_head, items=mentions)
            file_name = f'{WEB_ROOT}/{self.date_str}/mentions.html'
            os.makedirs(os.path.dirname(file_name), exist_ok=True)
            with open(file_name, 'w') as file:
                file.write(mentions_html_content)

        if trends is not None and mentions is not None:
            html_content = main_template.render(title=t.image_head,
                                                trends_items=trends[:10],
                                                mentions_items=mentions[:10],
                                                trend_title=trends_title,
                                                mention_title=mentions_title)
            file_name = f'{WEB_ROOT}/{self.date_str}/index.html'
            os.makedirs(os.path.dirname(file_name), exist_ok=True)
            with open(file_name, 'w') as file:
                file.write(html_content)
            trends = None


    @staticmethod
    def sorted_subitems(tweet):
        items = [x.subitems for x in tweet.items]
        subitems = []
        for x in items:
            subitems.extend(x)
        return sorted(subitems, key=lambda si: si.score, reverse=True)

    @staticmethod
    def get_pending(env):
        url = env.base_url + 'getpending?account=' + env.default_account
        response = urllib.request.urlopen(url, timeout=60)
        the_page = response.read()

        data = json.loads(the_page)
        print(data)
        return data
