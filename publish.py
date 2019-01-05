# This version uses backup pic if error obtaining profile pic
# 2014-04-03 Switched to urdu_reshaper
import twitter
from twitter import TwitterError

import defaults
import re
import os
import fnmatch
import time
import datetime
from PIL import ImageFont
from PIL import Image
from PIL import ImageDraw
import io
import os.path
from twitterstats import urdu_reshaper
from bidi.algorithm import get_display
import urllib.request
from templates import template_2017 as template
import logging

from twitterstats.dbsummary import DBSummary

logger = logging.getLogger('publish')


class Publish:
    def __init__(self, environment, db_summary, twitter_api):
        self.env = environment
        self.db_summary = db_summary
        self.twitter_api = twitter_api

        self.scoreWeight = {'trenders': 100,
                            'trends': 250,
                            'mentions': 25}

        # self.c.execute('select screen_name, token, token_secret from twitter_account')
        # rows = self.c.fetchall()
        #
        # self.accounts = dict()
        # for (screen_name, token, secret) in rows:
        #     acc = {'token': token, 'secret': secret}
        #     self.accounts[screen_name] = acc

    @staticmethod
    def write_tweet(tweet, count):
        result = []
        item_number = 1
        for item in tweet.items:
            # logger.info(f'{item.rank} {item.subrank} {item.tweet_text}')
            if item.subrank is None or item.subrank == 1:
                result.append(str(item_number) + " " + item.tweet_text)
                item_number += 1
            if item_number > count:
                break
        return tweet.head + '\n'.join(result) + tweet.tail

    def retweet(self, tweet_id):
        result = {'status': 'OK'}
        try:
            self.twitter_api.PostRetweet(tweet_id)
        except ConnectionError:
            print('Connection error.')
            raise
        except TwitterError as e:
            result = {'status': 'TwitterError',
                      'Message': 'Error {}: {}'.format(e.message[0]['code'], e.message[0]['message'])}
            logger.warning('Error {}: {}'.format(e.message[0]['code'], e.message[0]['message']))
        else:
            logger.info('Retweeted %s' % tweet_id)
            self.db_summary.save_retweet(tweet_id)

        return result

    @staticmethod
    def use_backup_image(screen_name, img, listed):
        files = fnmatch.filter(os.listdir('profiles_bak'), screen_name + '.*')
        if files is None or len(files) == 0:
            logger.debug("Backup file for %s does not exist.", screen_name)
            return
        picfile = "profiles_bak/" + files[0]
        logger.debug(picfile)
        try:
            if os.path.isfile(picfile):
                pic = Image.open(picfile)
                picr = pic.resize((54, 54), Image.ANTIALIAS)
                img.paste(picr, (455, 125 + (listed * 60)))
        except:
            logger.error("Error loading image: " + picfile)
            raise

    @staticmethod
    def resize_rect(rect, size):
        x, y = rect

        if x < y:
            nx = size
            ny = y * nx / x
        else:
            ny = size
            nx = x * ny / y

        return nx, ny

    @staticmethod
    def print_text(text, position, colour, draw, font, shadow=True):
        try:
            reshaped_text = urdu_reshaper.reshape(text)
            bidi_text = get_display(reshaped_text)
            if shadow:
                draw.text((position[0] + 2, position[1] + 2), bidi_text, template.SHADOW_COLOUR, font=font)
            draw.text(position, bidi_text, colour, font=font)
        except AssertionError:
            logger.error("Unable to display text %s" % text)

    def print_text_box(self, text, position_top_right, colour, draw, font, shadow=True):
        words = text.split(' ')
        max_width = 0
        max_height = 0
        widths = []
        for word in words:
            w, h = draw.textsize(word, font=font)
            widths.append(w)
            max_width = max(w, max_width)
            max_height = max(h, max_height)

        x_center = position_top_right[0] - (max_width / 2)
        while True:
            for i, word in enumerate(words):
                if (i < len(words) - 1
                        and word != ""
                        and sum(widths[i:i + 1]) < max_width):
                    combined = ' '.join(words[i:i + 2])
                    w, h = draw.textsize(combined, font=font)
                    if w <= max_width:
                        words[i] = combined
                        widths[i] = w
                        words[i + 1] = ""
                        widths[i + 1] = 0
            new_words = list(filter(None, words))
            new_widths = list(filter(None, widths))
            logger.info('Len: {} {}'.format(len(words), len(new_words)))
            if len(words) == len(new_words):
                break
            words = new_words
            widths = new_widths

        for i, line in enumerate(new_words):
            self.print_text(line,
                            (x_center - new_widths[i] / 2, position_top_right[1] + i * max_height),
                            colour,
                            draw,
                            font,
                            shadow)

    @staticmethod
    def split_file_name(filename, delimiter='_'):
        # Get the extension
        if '.' in filename:
            parts = filename.split('.')
            name = '.'.join(parts[:-1])
            ext = parts[-1]
        else:
            name = filename
            ext = ''

        filedate = None
        if delimiter in name:
            parts = name.split(delimiter)
            filedate = parts[0]
            name = delimiter.join(parts[1:])

        return filedate, name, ext

    def draw_tweet(self, tweet, star_weight):
        global img_resized
        font = ImageFont.truetype("fonts/Arial Unicode.ttf", 50)
        fontb = ImageFont.truetype("fonts/Arial Unicode.ttf", 40)
        font_image_caption = ImageFont.truetype("fonts/Arial Unicode.ttf", 35)
        starw = Image.open("images/start40.png")
        starg = Image.open("images/start40g.png")
        starr = Image.open("images/start40red.png")
        starsh = Image.open("images/start40s60shl.png")

        action = tweet.type

        # star = staro.resize((40,40), Image.ANTIALIAS)
        # img=Image.new("RGBA", (600,600),(120,20,20))
        bgimg = Image.open("images/" + template.BACKGROUND_PIC)
        width, height = bgimg.size
        img = Image.new("RGBA", (width, height))
        draw = ImageDraw.Draw(img)
        img.paste(bgimg, (0, 0))

        image_url = tweet.background_image
        if image_url is not None and image_url != "" and os.path.exists("images/custom/%s" % image_url):
            image_url = "images/custom/%s" % image_url
        dateimage = None
        image_text = None
        if action == 'mentions':  # Load the image for the date if available
            for file in os.listdir("images/calendar"):
                filedate, name, ext = self.split_file_name(file)
                # print "Cal", file[:5], tweet.date_nkey'][-5:]
                if filedate is not None and (
                        filedate == tweet.date_nkey
                        or (dateimage is None and filedate == tweet.date_nkey[-5:])):
                    dateimage = "images/calendar/%s" % file
                    image_text = name

        if dateimage is not None:
            image_url = dateimage
        elif image_url is not None and '|' in image_url:
            image_url, image_text = image_url.split('|')[:2]

        if image_url is not None and image_url != "":
            try:
                if os.path.exists("images/custom/%s" % image_url):
                    pic2 = Image.open("images/custom/%s" % image_url)
                else:
                    file = io.BytesIO(urllib.request.urlopen(image_url, timeout=10, retries=2).read())
                    pic2 = Image.open(file)
                pic = pic2.point(lambda p: p * 0.7)
                # pWidth, pHeight = pic.size

                new_size = self.resize_rect(pic.size, 680)
                picr = pic.resize(new_size, Image.ANTIALIAS)
                img.paste(picr, (width - ((680 + new_size[0]) / 2), 100 - ((new_size[1] - 680) / 2)))

                if image_text is not None:
                    self.print_text_box(image_text, (width - 16, 100), template.IMAGE_CAPTION_COLOUR, draw,
                                        font_image_caption,
                                        True)
            except IOError:
                logger.error("Unable to load image: %s", image_url)

        picr = Image.open("images/" + template.FOREGROUND_PIC)
        img.paste(picr, (0, 0), picr)

        # print "head: ", head, background_pic
        showdate = datetime.datetime.strptime(tweet.date_nkey, '%Y-%m-%d').strftime('%d %B %Y').lstrip('0')

        self.print_text(tweet.image_head, (40, 20), template.HEADING_COLOUR, draw, font, False)
        # 		try:
        # 			reshaped_text = urdu_reshaper.reshape(tweet.image_head'])
        # 			bidi_text = get_display(reshaped_text)
        # 			draw.text((40, 20),bidi_text,HEADING_COLOUR,font=font)
        # 		except AssertionError:
        # 			logger.error("Unable to display title: " + tweet.image_head'])
        w, h = draw.textsize(showdate, font=fontb)
        draw.text(((width - w) - 50, 40), showdate, template.DATE_COLOUR, font=fontb)
        draw = ImageDraw.Draw(img)
        # draw = ImageDraw.Draw(img)

        i = 0
        listed = 0
        logger.debug("Len %d" % len(tweet.items))
        # rank = -1
        while i < len(tweet.items) and listed < 10:
            if tweet.items[i].tweet_text == '':
                i += 1
                continue
            score = tweet.items[i].score
            # url = None
            if tweet.type != 'trends':
                textbuffer = 80
                # (url, name) = get_tweeter_details(tweeps[i][0])
                if tweet.items[i].display_text != '':
                    w, h = draw.textsize(tweet.items[i].tweet_text, font=fontb)
                    self.print_text(tweet.items[i].display_text, (450 + textbuffer + w + 20, 130 + (listed * 60)),
                                    template.NAME_COLOUR, draw, fontb)
            # 				try:
            # 					reshaped_text = urdu_reshaper.reshape(tweet.items'][i]['display_text'])
            # 					bidi_text = get_display(reshaped_text)
            # 					draw.text((450 + textbuffer + w + 20, 130 + (listed * 60)),bidi_text,NAME_COLOUR,font=fontb)
            # 				except AssertionError:
            # 					print "Unable to display name for", tweet.items'][i]['tweet_text']
            else:
                textbuffer = 0
            w, h = draw.textsize(str(listed + 1), font=fontb)
            self.print_text(str(listed + 1), (430 - w, 130 + (listed * 60)), template.RANK_COLOUR, draw, fontb)
            # draw.text((430 - w, 130 + (listed * 60)),str(listed + 1),RANK_COLOUR,font=fontb)

            if i < 7:
                widthlimit = 1100
            else:
                widthlimit = 900
            newtext = tweet.items[i].tweet_text
            while i < len(tweet.items) - 1 and tweet.items[i].rank == tweet.items[i + 1].rank:
                i += 1
                newtext += ' ' + tweet.items[i].tweet_text
            # todo join same trends
            w, h = draw.textsize(newtext, font=fontb)
            while w > widthlimit:
                newtext = newtext.rsplit(' ', 1)[0]
                w, h = draw.textsize(newtext, font=fontb)
            self.print_text(newtext, (450 + textbuffer, 130 + (listed * 60)), template.SCREEN_NAME_COLOUR, draw, fontb)
            # 		try:
            # 			reshaped_text = urdu_reshaper.reshape(newtext)
            # 			bidi_text = get_display(reshaped_text)
            # 			draw.text((450 + textbuffer, 130 + (listed * 60)),bidi_text,SCREEN_NAME_COLOUR,font=fontb)
            # 		except AssertionError:
            # 			print "Unable to display trends: ", newtext

            starcount = int(score / star_weight)
            distance = 35
            redstars = int(starcount / 25)
            goldstars = int((starcount % 25) / 5)
            whitestars = starcount % 5

            # Shadows
            starx = 280
            for s in range(redstars + goldstars + whitestars):
                img.paste(starsh, (int(starx), 125 + (listed * 60)), mask=starsh)
                starx -= distance

            if starcount > 0:
                w, h = draw.textsize(str(starcount), font=fontb)
                self.print_text(str(starcount), (380 - w, 130 + (listed * 60)), template.SCORE_COLOUR, draw, fontb)
            # draw.text((400 - w, 130 + (listed * 60)),str(starcount),SCORE_COLOUR,font=fontb)

            # Stars
            starx = 290
            for s in range(redstars):
                img.paste(starr, (int(starx), 135 + (listed * 60)), mask=starw)
                starx -= distance
            for s in range(goldstars):
                img.paste(starg, (int(starx), 135 + (listed * 60)), mask=starw)
                starx -= distance
            for s in range(whitestars):
                img.paste(starw, (int(starx), 135 + (listed * 60)), mask=starw)
                starx -= distance

            # Draw profile pictures
            if tweet.type != 'trends':
                # if url == None:
                # i += 1
                # continue
                if tweet.items[i].display_image is not None:
                    try:
                        m = re.search('(\.[^.]+)$', tweet.items[i].display_image)
                        picfile = "profiles/" + tweet.items[i].tweet_text[1:] + m.group(1)
                        logger.debug(picfile)
                        if os.path.isfile(picfile):
                            pic = Image.open(picfile)
                        else:
                            file = io.BytesIO(urllib.request.urlopen(tweet.items[i].display_image,
                                                                     timeout=30).read())
                            # print "image:", tweet.items'][i]['display_image']
                            pic = Image.open(file)
                            pic.save(picfile)
                        picr = pic.resize((54, 54), Image.ANTIALIAS)
                        img.paste(picr, (455, 125 + (listed * 60)))
                    except:
                        logger.error("Error loading image: %s, %s", tweet.items[i].display_image, picfile)
                        self.use_backup_image(tweet.items[i].tweet_text[1:], img, listed)

            i += 1
            listed += 1
        img_resized = img.resize((1050, 520), Image.ANTIALIAS).convert('RGB')

        img_file = 'output/' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '_' + action
        if action == 'trenders':
            ht = re.search('#(.*):', tweet.head)
            if ht is not None:
                img_file += '_' + ht.group(1)
        img_file += '.jpg'
        img_resized.save(img_file, quality=95, optimize=True)
        return img_file


def main():
    env = defaults.get_environment()
    db_summary = DBSummary(env)

    tweets = db_summary.get_pending_tweets()

    token = db_summary.get_default_token()
    twitter_api = twitter.Api(consumer_key=env.consumer_key,
                              consumer_secret=env.consumer_secret,
                              access_token_key=token.key,
                              access_token_secret=token.secret)

    pub = Publish(env, db_summary, twitter_api)
    # tweetbucket = list()
    tcount = 0
    for tweet in tweets:
        tcount += 1
        if tweet.status == 'pend-rej':
            tweet.status = 'rejected'
            tweet.posted_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        elif tweet.type == 'retweet':
            if tcount > 1:
                time.sleep(env.tweet_delay)
            result = dict()
            if env.post == 'post':
                result = pub.retweet(tweet.tweet_id)
            else:
                logger.debug('Tweet %s not posted due to env setting.', tweet.tweet_id)
            if 'status' in result and result['status'] != 'OK':
                tweet.status = result['message']
            else:
                tweet.status = 'posted'
            tweet.posted_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        else:
            if tcount > 1:
                time.sleep(env.tweet_delay)
            # weight_multiplier = 1

            action = tweet.type
            i = 10
            tweet_text = pub.write_tweet(tweet, i)
            while len(tweet_text) > 145 and i > 1:
                i -= 1
                tweet_text = pub.write_tweet(tweet, i)

            if not env.production:
                tweet_text = tweet_text.replace('#', '-').replace('@', '+')

            img_file = pub.draw_tweet(tweet, pub.scoreWeight[action])

            logger.info('%3d %s', len(tweet_text), tweet_text)

            if env.post == 'post':
                photo = open(img_file, 'rb')
                result = twitter_api.PostUpdate(media=photo, status=tweet_text)
                print(result)
                logger.info('Posted!')

            tweet.status = 'posted'
            tweet.posted_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        tweet.save_status()

        # tweetbucket.append(tweet)
        # if len(tweetbucket) >= 3:
        #     data = {'tweets': tweetbucket}
        #     Publisher.publish(env, data, 'posted')
        #     tweetbucket = list()
        #     time.sleep(10)

    # if len(tweetbucket) > 0:
    #     data = {'tweets': tweetbucket}
    #     Publisher.publish(env, data, 'posted')

    db_summary.disconnect()


if __name__ == '__main__':
    import logging.config
    import yaml
    # import TlsSMTPHandler

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
