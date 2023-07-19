#!./python

import Table

# contains data definition information for the antibody_to_reference table

###--- Globals ---###

# name of this database table
tableName = 'antibody_to_reference'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int     NOT NULL,
        antibody_key    int     NOT NULL,
        reference_key   int     NOT NULL,
        qualifier       text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'reference_key' : 'create index %s on %s (reference_key)',
        }

keys = {
        'antibody_key' : ('antibody', 'antibody_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('antibody_key', 'create index %s on %s (antibody_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between the antibody and reference flowers',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record, no other purpose',
                'antibody_key' : 'identifies the antibody',
                'reference_key' : 'identifies the reference',
                'qualifier' : 'qualifier describing the association (often null)',
                },
        Table.INDEX : {
                'reference_key' : 'look up all antibodies for a reference',
                'antibody_key' : 'clusters data so all references for an antibody are stored together on disk, to aid quick retrieval',
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
