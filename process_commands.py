import logging

from twitter import TwitterError

import defaults
from datetime import timedelta
import datetime
from twitterstats.db import DB
from twitterstats.dbsummary import DBSummary
from twitterstats.publisher import Publisher
from twitterstats.secommon import today, now, nvl
from twitterstats.secommon import yesterday
from twitterstats.secommon import yesterday_file
from subprocess import call

from twitterstats.twitterapi import TwitterAPI

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

    api = TwitterAPI(environment, db_summary)

    commands = db.get_commands(screen_names=db_summary.get_account_screen_names())
    processed_commands = db_summary.get_processed_commands(since_id=db.get_baseline_tweet_id())

    for command in commands:
        if command.id in processed_commands:
            logger.info(f'Skipping {command.id}. Already processed: {command.text}')
        else:
            if command.text.lower()[:5] == 'add #':
                tag_name = command.text[5:]
                logger.info(f'Adding {tag_name}')
                call('python3.7 words.py ' + tag_name, shell=True)
                tag = db.get_tag_ranges(tag=f'#{tag_name}', min_override=db.get_baseline_tweet_id())
                print(tag.name_scores)
                name_score = tag.name_scores[-2] if len(tag.name_scores) > 1 else None
                score_text = '{} / {} = {:.1f}'.format(name_score.total_score,
                                                       name_score.status_count,
                                                       name_score.total_score / max(name_score.status_count, 1)
                                                       ) if name_score is not None else ''
                status_text = f'-{tag_name} added. {score_text} {tag.state}'
                logger.info(status_text)
                try:
                    api.polling_api().PostUpdate(status_text,
                                                 in_reply_to_status_id=command.id)
                except TwitterError as e:
                    logger.error(e.message)
                command.status = status_text
                command.processed_date = now()
                db_summary.save_command(command)
                db_summary.commit()
            else:
                logger.info(f'Unknown command {command.id}: {command.text}')

    db_summary.disconnect()
    db.disconnect()


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
