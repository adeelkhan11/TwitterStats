import logging
import defaults
from datetime import timedelta
import datetime
from twitterstats.db import DB
from twitterstats.dbsummary import DBSummary
from twitterstats.publisher import Publisher
from twitterstats.secommon import today, now, nvl
from twitterstats.secommon import yesterday
from twitterstats.secommon import yesterday_file

logger = logging.getLogger('getpending')


def main():
    # parser = argparse.ArgumentParser(description='Draft stats for the given day and push to cloud for approval.')
    # parser.add_argument('date', metavar='yyyy-mm-dd',
    #                     help='the date to process')
    #
    # args = parser.parse_args()

    environment = defaults.get_environment()
    db = DB(environment, today())
    db_summary = DBSummary(environment)

    jdata = Publisher.get_pending(environment)

    # c = db_connect(env.summary_database)
    trenders_published = list()
    trenders_all = list()
    already_processed = list()
    if 'tweets' in jdata:
        for tweet in jdata['tweets']:
            tweet_status = db_summary.get_tweet_status(tweet['t_id'])
            if tweet_status is None:
                db_summary.save_tweet(tweet)
                for item in tweet['items']:
                    db_summary.save_tweet_item(tweet, item)
                    if tweet['type'] == 'trenders' and item['selected'] == 'Y':
                        trenders_all.append(item['tweet_text'][1:])
                        if tweet['status'] == 'pend-post':
                            trenders_published.append(item['tweet_text'][1:])
            elif tweet_status in ['posted', 'rejected']:
                tweet['status'] = tweet_status
                already_processed.append(tweet)

        if len(trenders_published) > 0:
            with open(f'{environment.bot_data_directory}/trenders_all_{yesterday_file()}.txt', 'a') as f:
                for sn in trenders_all:
                    f.write("%s\n" % sn)
            with open(f'{environment.bot_data_directory}/trenders_published_{yesterday_file()}.txt', 'a') as f:
                for sn in trenders_published:
                    f.write("%s\n" % sn)

    db_summary.disconnect()

    trend_date = now()
    # now = now()
    # yesterday = (datetime.datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    if 'trends' in jdata:
        if len(jdata['trends']) > 0:
            # c = db_connect()

            # t = (yesterday,)
            # c.execute('SELECT max(max_id) max_id FROM tag_history where date <= ?', t)
            # row = c.fetchone()
            # max_id = 0
            # if row != None:
            #     max_id = row[0]

            relevant_words = db.get_relevant_words()

            generic_words = db.get_generic_words()

            trs = list()
            for trend in jdata['trends']:
                tag = '#' + trend['hashtag'].lower()
                tr = {'hashtag': trend['hashtag'], 'status': 'posted', 'trend_at': trend_date}
                trs.append(tr)
                tag_discovery_result = db.get_tag_discovery_result(tag)
                status = nvl(tag_discovery_result, 'NONE')

                if trend['status'] == 'pend-post' and status in ('NONE', 'AUTO_DEL', 'MAN_DEL'):
                    logger.info('Adding: ' + tag)
                    db.save_tag_discovery(tag, 'MAN_ADD')
                elif trend['status'] == 'pend-del' and status in ('AUTO_ADD', 'MAN_ADD'):
                    logger.info('Deleting: ' + tag)
                    db.save_tag_discovery(tag, 'MAN_DEL')

                # Trend relevance
                if 'relevance' in trend:
                    relevance = relevant_words[
                        trend['hashtag'].lower()] if trend['hashtag'].lower() in relevant_words else 'neutral'
                    if trend['relevance'] != relevance:
                        new_relevance = None if trend['relevance'] == 'neutral' else trend['relevance']
                        db.set_word_relevance(trend['hashtag'], new_relevance)

                # Trend generic
                if 'generic' in trend:
                    generic = generic_words[trend['hashtag'].lower()] if trend[
                                                                             'hashtag'].lower() in generic_words else ''
                    if trend['generic'] != generic:
                        new_relevance = None if trend['generic'] == 'neutral' else trend['generic']
                        db.set_word_generic(trend['hashtag'], new_relevance)

            data = {'trends': trs}
            Publisher.publish(environment, data, 'trends')

            db.commit()
            open(f'{environment.temp_file_directory}/compute_daily', 'a').close()

    if 'categories' in jdata:
        if len(jdata['categories']) > 0:
            for cat in jdata['categories']:
                db.set_tweeter_category(cat['screen_name'], cat['category'])
                logger.info("Category for", cat['screen_name'], "changed to", cat['category'])

            db.commit()

    if 'words' in jdata:
        if len(jdata['words']) > 0:
            for word in jdata['words']:
                category = word['category']
                if category == '':
                    category = None
                db.set_word_generic(word['word'], category)
                logger.info("Generic for", word['word'], "changed to", category)

    db.disconnect()

    if len(already_processed) > 0:
        data = {'tweets': already_processed}
        Publisher.publish(environment, data, 'posted')


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
