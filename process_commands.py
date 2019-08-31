import logging
import re

from twitter import TwitterError

import defaults
from twitterstats.db import DB
from twitterstats.dbsummary import DBSummary
from twitterstats.secommon import today, now
from subprocess import call

from twitterstats.twitterapi import TwitterAPI

logger = logging.getLogger('process_commands')


def save_command(command, status_text, db_summary, polling_api):
    logger.info(f'{command.id}: {status_text}')
    try:
        polling_api.PostUpdate(status_text,
                               in_reply_to_status_id=command.id,
                               auto_populate_reply_metadata=True)
    except TwitterError as e:
        logger.error(e.message)
    command.status = status_text
    command.processed_date = now()
    db_summary.save_command(command)
    db_summary.commit()


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
            m = re.match('\+([a-zA-Z0-9_]+) ([A-Z][AB]?)( t ([0-9]+))?( dl ([0-9]+))?', command.text)
            if m:
                screen_name = m.group(1)
                category = m.group(2)
                rt_threshold = m.group(4)
                rt_daily_limit = m.group(6)

                db.set_tweeter_category(screen_name=screen_name,
                                        category=category,
                                        rt_threshold=rt_threshold,
                                        rt_daily_limit=rt_daily_limit)

                status_text = f'+{screen_name} set to {category}'
                if rt_threshold is not None:
                    status_text += f' rt threshold {rt_threshold}'
                if rt_daily_limit is not None:
                    status_text += f' dl {rt_daily_limit}'
                save_command(command, status_text, db_summary, api.polling_api())
            elif command.text.lower()[:5] == 'add #':
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
                save_command(command, status_text, db_summary, api.polling_api())
            else:
                if command.text[:2] not in ('To', 'RT'):
                    logger.info(f'Unknown command {command.id}: {command.text}')

    db_summary.disconnect()
    db.disconnect()


if __name__ == '__main__':
    import logging.config
    import yaml

    logging.config.dictConfig(yaml.load(open('logging.yaml', 'r')))
    main()
