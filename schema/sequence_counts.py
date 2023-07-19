#!./python

import Table

# contains data definition information for the sequence_counts table

###--- Globals ---###

# name of this database table
tableName = 'sequence_counts'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        sequence_key    int     NOT NULL,
        marker_count    int     NULL,
        probe_count     int     NULL,
        PRIMARY KEY(sequence_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'sequence_key' : ('sequence', 'sequence_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the sequence flower, containing pre-computed counts for each sequence',
        Table.COLUMN : {
                'sequence_key' : 'identifies the sequence',
                'marker_count' : 'count of associated markers',
                'probe_count' : 'count of associated probes',
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
