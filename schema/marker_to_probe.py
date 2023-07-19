#!./python

import Table

# contains data definition information for the marker_to_probe table

###--- Globals ---###

# name of this database table
tableName = 'marker_to_probe'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             NOT NULL,
        marker_key      int             NOT NULL,
        probe_key       int             NOT NULL,
        reference_key   int             NOT NULL,
        qualifier       text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'probe_key' : 'create index %s on %s (probe_key, marker_key)',
        'reference_key' : 'create index %s on %s (reference_key)',
        }

keys = {
        'marker_key' : ('marker', 'marker_key'),
        'probe_key' : ('probe', 'probe_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('marker_key',
        'create index %s on %s (marker_key, probe_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between the marker and probe flowers, associating a marker with its probes',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record, no other purpose',
                'marker_key' : 'identifies the marker',
                'probe_key' : 'identifies a probe for this marker',
                'reference_key' : 'identifies the reference for this association',
                'qualifier' : 'qualifier describing this association',
                },
        Table.INDEX : {
                'probe_key' : 'retrieve markers by a probe',
                'marker_key' : 'clusters data so all probe associations for a marker are grouped together on disk, to aid quick retrieval',
                'reference_key' : 'retrieve all marker/probe associations supported by a given reference',
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
