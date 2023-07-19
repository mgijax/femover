#!./python

import Table

# contains data definition information for the test_stats table
# this table is used exclusively for the purpose of pre-generating test data by querying pub (mgd).

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'test_stats'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        test_stats_key          int             not null,
        id                      text            not null,
        group_type                      text            not null,
        description             text            not null,
        sql_statement           text            not null,
        test_data               text            null,
        error                   text            null,
        PRIMARY KEY(test_stats_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'id' : 'create index %s on %s (id)',
        'group_type' : 'create index %s on %s (group_type)',
        }
keys = {}


clusteredIndex=None
# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'Stores pre-generated test data queried from pub.mgd',
        Table.COLUMN : {
                'test_stats_key' : 'unique identifier for this table',
                'id' : 'A unique text identifier for a row in this table',
                'group_type' : 'group of this id (for organising)',
                'description' : 'A brief description of what data this query generates',
                'sql_statement' : 'the sql query to be executed, whose result will be store in test_data',
                'test_data' : 'The return value of the query. Data could be numeric, text, or a custom object format. It is up to the test software to interpret the value in this column',
                'error' : 'error message if test_data could not be generated for any reason',
                },
        Table.INDEX : {
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
                clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
