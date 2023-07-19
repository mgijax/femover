#!./python

import Table

# contains data definition information for the sequence_clone_collection table

###--- Globals ---###

# name of this database table
tableName = 'sequence_clone_collection'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             NOT NULL,
        sequence_key    int             NOT NULL,
        collection      text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'sequence_key' : ('sequence', 'sequence_key') }

# index used to cluster data in the table
clusteredIndex = ('sequence_key', 'create index %s on %s (sequence_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the sequence flower, containing clone collections for each sequence (may have multiple records per sequence)',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this sequence/collection pair',
                'sequence_key' : 'identifies the sequence',
                'collection' : 'name of the clone collection',
                },
        Table.INDEX : {
                'sequence_key' : 'clusters data so all collections for a sequence are stored near each other on disk, for quick access',
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
