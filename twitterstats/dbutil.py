"""
Created on 18 Sep 2018

@author: Adeel Khan
"""

# import sqlite3
import datetime
import os
import logging
# import oauth2 as oauth
import re
# from dataclasses import dataclass, field

# from dataclasses_json import dataclass_json


logger = logging.getLogger(__name__)


class DBUtil:
    """
    With specific functions to connect to TwitterStats databases.
    """

    # c = None
    # conn = None
    DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, environment):
        """
        Constructor
        """
        self.env = environment
        self.c = None
        self.conn = None

    @staticmethod
    def get_file_date(date):
        return date.replace('-', '_')

    @staticmethod
    def get_file_month(date):
        return date[:7].replace('-', '_')

    def se_database(self, db_type, date='2000-01-01'):
        filename = None
        if db_type in ['se', 'se_dimension']:
            filename = (self.env.database_old if db_type == 'se' else self.env.dimension_database_old).format(
                self.get_file_date(date) if db_type == 'se' else self.get_file_month(date))
            if not os.path.isfile(filename):
                filename = self.env.database if db_type == 'se' else self.env.dimension_database
        elif db_type == 'se_summary':
            filename = self.env.summary_database
        return filename

    def commit(self):
        self.conn.commit()

    def disconnect(self):
        self.conn.commit()
        self.conn.close()

    @staticmethod
    def null_to_empty(value):
        if value is None:
            return ''
        return str(value)

    @staticmethod
    def null_to_zero(value):
        if value is None:
            return 0.0
        if isinstance(value, str):
            if value.strip() == '':
                return 0.0
            else:
                return float(value.replace(',', ''))
        return value

    @classmethod
    def sum_nullables(cls, values):
        # print "Summing:", values
        total = 0
        for v in values:
            total += cls.null_to_zero(v)
        return total

    def check_exists(self, table, keys, values):
        where_clause = ' and '.join(["%s = ?" % k for k in keys])
        sql = "select count(*) from %s where %s" % (table, where_clause)
        self.c.execute(sql, tuple(values))
        result = self.c.fetchone()
        return result[0] > 0

    @classmethod
    def to_datetime(cls, text):
        return datetime.datetime.strptime(text, cls.DATETIME_FORMAT)

    @classmethod
    def datetime_to_str(cls, dt):
        return dt.strftime(cls.DATETIME_FORMAT)

    @staticmethod
    def get_dict_key(my_dict, keys):
        return '_'.join([str(my_dict[k]) for k in keys])

    @classmethod
    def convert_rows_to_dict(cls, rows, keys, columns):
        all_columns = keys + columns
        column_count = len(all_columns)
        rows_dict = dict()
        for row in rows:
            row_dict = dict()
            for i in range(column_count):
                row_dict[all_columns[i]] = row[i] if all_columns[i] not in keys else str(row[i])
            rows_dict[cls.get_dict_key(row_dict, keys)] = row_dict
            # print self.getDictKey(row_dict, keys), row_dict
        return rows_dict

    def pivot_dicts(self, data, row_keys, column_keys, value):
        result = dict()
        for row in data:
            row_key = self.get_dict_key(row, row_keys)
            col_key = self.get_dict_key(row, column_keys)

            if row_key not in result:
                result[row_key] = dict()

            result[row_key][col_key] = row[value]
        return result

    def load_dicts(self, table, keys, columns, filter_text="?=?", filter_values=(1, 1)):
        all_columns = keys + columns
        # print "All columns", all_columns, keys, columns
        sql = "select %s from %s where %s" % (', '.join(all_columns), table, filter_text)
        self.c.execute(sql, tuple(filter_values))
        rows = self.c.fetchall()

        return self.convert_rows_to_dict(rows, keys, columns)

    def load_dicts_from_query(self, keys, columns, sql, filter_values=None):
        if filter_values is None:
            self.c.execute(sql)
        else:
            self.c.execute(sql, tuple(filter_values))
        rows = self.c.fetchall()

        return self.convert_rows_to_dict(rows, keys, columns)

    def save_dict(self, table, keys, rec, update_only=False):
        columns = rec.keys()
        values = [rec[k] for k in columns]
        if keys is not None and self.check_exists(table, keys, [rec[k] for k in keys]):
            set_clause = ', '.join(["%s = ?" % k for k in columns if k not in keys])
            where_clause = ' and '.join(["%s = ?" % k for k in keys])
            sql = "update %s set %s where %s" % (table, set_clause, where_clause)
            # print sql
            update_values = [rec[k] for k in columns if k not in keys]
            update_values.extend([rec[k] for k in keys])
            # print update_values
            self.c.execute(sql, tuple(update_values))
        elif update_only:
            logger.warning("Warning: Value %s not found in table %s. Cannot update.", '-'.join([rec[k] for k in keys]),
                           table)
            return False
        else:
            sql = "insert into %s (%s) values (%s)" % (table, ', '.join(columns), ', '.join(['?'] * len(columns)))
            # print sql
            self.c.execute(sql, values)
        return True

    @staticmethod
    def calculate_text_score(text, score_table):
        result = 0
        if text is not None:
            words = re.split('[^A-Za-z]+', text.lower())

            for n in words:
                if n in score_table:
                    result += score_table[n]

        return result

    @staticmethod
    def none_to_empty(text):
        return text if text is not None else ''
