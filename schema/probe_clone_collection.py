#!./python

import Table

# contains data definition information for the probe_clone_collection table

###--- Globals ---###

# name of this database table
tableName = 'probe_clone_collection'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             NOT NULL,
        probe_key       int             NOT NULL,
        collection      text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'probe_key' : ('probe', 'probe_key') }

# index used to cluster data in the table
clusteredIndex = ('probe_key', 'create index %s on %s (probe_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table in the probe flower, containing clone collections associated with each probe',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record',
                'probe_key' : 'identifies the probe',
                'collection' : 'name of the clone collection',
                },
        Table.INDEX : {
                'probe_key' : 'clusters data to group all clone collections for a probe together on disk for quick access',
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
