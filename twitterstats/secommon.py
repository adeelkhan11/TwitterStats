# import env
import json
import urllib
from urllib.request import Request, urlopen
# import urllib2
import datetime
import csv
import operator
import os
# import re
import logging

# from scipy.optimize.optimize import Result


log_level = 2
log_sequence = 0

logger = logging.getLogger(__name__)


def get_logger(script="pps"):
    # create logger
    logger = logging.getLogger(script)
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger


def post_to_portal(type, jdata, env):
    jtext = json.dumps(jdata)
    # log("-" * 50)
    logger.debug("PostToPortal request (length=%i): %s", len(jtext), jtext)

    url = env.base_url + 'deliver'
    values = {'data': jtext,
              'type': type,
              'account': env.DEFAULT_ACCOUNT}

    data = urllib.urlencode(values)
    req = Request(url, data)
    response = urlopen(req)
    the_page = response.read()

    # logger.debug("-" * 50)
    logger.info("PostToPortal response: %s", the_page)


# log("-" * 50)

# 0 Error
# 1 Warning
# 2 Informational
# 3 Debugging
def log(message, level=0):
    global log_level, log_sequence
    log_sequence += 1
    msg = "%s %5d  %s" % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), log_sequence, message)
    if level <= log_level:
        print
        msg
    logfile = 'log/log_%s.txt' % datetime.datetime.now().strftime('%Y_%m_%d')
    with open(logfile, "a") as myfile:
        myfile.write((msg + u"\n").encode('utf8'))


def log_error(message):
    msg = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S Error\n') + message + "\n"
    print
    msg
    logfile = 'log/log_%s_error.txt' % datetime.datetime.now().strftime('%Y_%m_%d')
    with open(logfile, "a") as myfile:
        myfile.write((msg + u"\n\n").encode('utf8'))


def read_csv_hash(file, removeZero=False):
    result = {}

    with open(file, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            if len(row) == 2 and (int(row[1]) != 0 or removeZero == False):
                result[row[0].lower()] = int(row[1])

    return result


def rank_words(myfile, days):
    words = dict()
    for i in range(days):
        filedate = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        filename = myfile % filedate
        if os.path.isfile(filename):
            with open(filename) as f:
                for full_line in f:
                    line = full_line.rstrip('\n')
                    parts = line.split(',')
                    for part in parts:
                        words[part] = words[part] + 1 if part in words else 1

    return [x[0] for x in sorted(words.items(), key=operator.itemgetter(1), reverse=True)]


def file_timestamp():
    return datetime.datetime.now().strftime('%Y_%m_%d_%H%M%S')


def now():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def today():
    return datetime.datetime.now().strftime('%Y-%m-%d')


def yesterday():
    return (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')


def save_list(mylists, filename):
    with open(filename, "w") as f:
        writer = csv.writer(f)
        for row in mylists:
            # writer.writerows(mylists)
            writer.writerow(row)


def get_db_filename(dbtype='se', date=None):
    filedate = '' if date is None else '_%s' % date.replace('-', '_')
    filename = "data/%s%s.db" % (dbtype, filedate)
    if not os.path.isfile(filename):
        logger.error('Database %s not found.', filename)
        filename = None
    return filename
