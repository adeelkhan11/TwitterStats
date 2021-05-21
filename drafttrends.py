import logging
import defaults
from subprocess import call
from twitterstats.db import DB
from twitterstats.secommon import today

logger = logging.getLogger('drafttrends')


class DraftTrends:
    def __init__(self, environment, db):
        self.environment = environment
        self.db = db
        sorted_x = self.get_trends()
        self.db.disconnect()

        i = 0
        new_trends = []
        while i < len(sorted_x) and sorted_x[i][1]['score'] >= 50:
            if sorted_x[i][1]['status'] == 'NONE' and len(sorted_x[i][0]) > 2:
                if len(new_trends) < 12:
                    logger.info('Calling ' + sorted_x[i][0])
                    call('python3.7 words.py ' + sorted_x[i][0], shell=True)
                new_trends.append(sorted_x[i][0])
            i += 1

        if len(new_trends) > 0:
            self.db.connect()
            sorted_x = self.get_trends()
            self.db.disconnect()

        i = 0
        logger.info('*** NEW TRENDS ***')
        while i < len(sorted_x):
            if (sorted_x[i][1]['score'] >= 50) or sorted_x[i][1]['status'] in ('AUTO_ADD', 'MAN_ADD'):
                if sorted_x[i][0] in new_trends:
                    logger.info(f'{sorted_x[i][1]["score"]:5d} {sorted_x[i][1]["status"]:<10} {sorted_x[i][0]}')
            i += 1

    def get_trends(self):
        rows = self.db.get_trends_relevance()

        all_trends = self.db.get_trends()
        # all_trend_times = self.db.get_trend_discovery_times()
        trends = {}
        for tag, tstatus in all_trends.items():
            word = tag.lower()[1:]
            trends[word] = {'score': 0, 'status': tstatus, 'trend': tag[1:]}
        for word, relevance, count in rows:
            found = False
            if len(word) > 5:
                for w, cnt in trends.items():
                    if w.lower() == word.lower():
                        found = True
                        trends[w]['score'] += count
                        trends[w]['relevance'] = relevance
            if not found:
                tstatus = 'NONE'
                if '#' + word.lower() in all_trends:
                    tstatus = all_trends['#' + word.lower()]
                trends[word.lower()] = {'score': count, 'status': tstatus, 'trend': word, 'relevance': relevance}

        # # Add related trends
        # rows = self.db.get_related_trends()
        # for row in rows:
        #     t, t2, count = row
        #     if t.lower() in trends:
        #         related = ''
        #         if 'related' in trends[t.lower()]:
        #             related = trends[t.lower()]['related']
        #
        #         if len(related) + len(t2) <= 40:
        #             status = ''
        #             if '#' + t2.lower() in all_trends:
        #                 if all_trends['#' + t2.lower()] in ('AUTO_ADD', 'MAN_ADD'):
        #                     status = '+'
        #                 elif all_trends['#' + t2.lower()] in ('AUTO_DEL', 'MAN_DEL'):
        #                     status = '-'
        #             trends[t.lower()]['related'] = related + ', ' + status + t2 + '(' + str(count) + ')'

        # rows = self.db.get_top_tag_scores()
        #
        # for tag, tweet_count, score in rows:
        #     t = tag.lower()[1:]
        #     if t in trends:
        #         trend = trends[t]
        #         tc = 0
        #         s = 0
        #         if 'tweet_count' in trend:
        #             tc = trend['tweet_count']
        #             s = trend['discovery_score']
        #         if tc < 300:
        #             trend['tweet_count'] = tc + tweet_count
        #             trend['discovery_score'] = s + score

        # # Add discovery times
        # for tag, trend in trends.items():
        #     if '#' + tag in all_trend_times:
        #         trend['discovery_time'] = all_trend_times['#' + tag]

        sorted_x = sorted(trends.items(), key=lambda x: x[1]['score'], reverse=True)
        return sorted_x


def main():
    # parser = argparse.ArgumentParser(description='Draft stats for the given day and push to cloud for approval.')
    # parser.add_argument('date', metavar='yyyy-mm-dd',
    #                     help='the date to process')
    #
    # args = parser.parse_args()

    environment = defaults.get_environment()
    db = DB(environment, today())

    _ = DraftTrends(environment, db)


if __name__ == '__main__':
    import logging.config
    import yaml

    # import TlsSMTPHandler

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
