#!./python

import Table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'glossary_term'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        glossary_key                    text            not null,
        display_name                    text            not null,
        definition              text            not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'glossary_key' : 'create index %s on %s (glossary_key)',
        }
keys = {}


clusteredIndex=None
# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains data from the glossary.rcd file',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this table',
                'glossary_key' : 'readable key for linking',
                'display_name' : 'glossary term display value',
                'definition' : 'glossary term definition',
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
