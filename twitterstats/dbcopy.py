import logging

logger = logging.getLogger('dbcopy')


class DBCopy:
    def __init__(self, old_cursor, new_cursor):
        self.old_cursor = old_cursor
        self.new_cursor = new_cursor

        self.copy_schema()
        
    def copy_table(self, table_name, source_query, source_query_parameters):
        logger.info(table_name)
        self.old_cursor.execute(f'PRAGMA table_info({table_name})')
        rows = self.old_cursor.fetchall()
        columns = [r[1] for r in rows]
        column_names = ', '.join(columns)
        data_markers = ', '.join(['?'] * len(column_names.split(',')))

        cnt = 0
        self.old_cursor.execute(source_query.format(column_names), source_query_parameters)
        row = self.old_cursor.fetchone()
        while row is not None:
            cnt += 1
            t = row
            self.new_cursor.execute(f'insert into {table_name} ({column_names}) values ({data_markers})', t)
            row = self.old_cursor.fetchone()
        logger.info(f'{cnt} rows')

    def copy_schema(self):
        # Tables
        t = ('table', 'sqlite_sequence')
        self.old_cursor.execute('select sql from sqlite_master where type = ? and name != ?', t)
        rows = self.old_cursor.fetchall()
        for sql in rows:
            self.new_cursor.execute(sql[0])

        # Indexes
        t = ('index',)
        self.old_cursor.execute('select sql from sqlite_master where type = ?', t)
        rows = self.old_cursor.fetchall()
        for sql in rows:
            self.new_cursor.execute(sql[0])
