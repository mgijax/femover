#!./python

import Table

# contains data definition information for the marker_to_term table

###--- Globals ---###

# name of this database table
tableName = 'marker_to_term'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             NOT NULL,
        marker_key      int             NOT NULL,
        term_key        int             NOT NULL,
        reference_key   int             NULL,
        qualifier       text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'marker_key' : 'create index %s on %s (marker_key, term_key)',
        'reference_key' : 'create index %s on %s (reference_key)',
        }

keys = {
        'marker_key' : ('marker', 'marker_key'),
        'term_key' : ('term', 'term_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('term_key',
        'create index %s on %s (term_key, marker_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between the marker and term flowers, associating a marker with its terms',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record, no other purpose',
                'marker_key' : 'identifies the marker',
                'term_key' : 'identifies a term for this marker',
                'reference_key' : 'identifies the reference for this association',
                'qualifier' : 'qualifier describing this association',
                },
        Table.INDEX : {
                'marker_key' : 'retrieve terms for a marker',
                'term_key' : 'clusters data so all marker associations for a term are grouped together on disk, to aid quick retrieval (as for PIRSF detail page)',
                'reference_key' : 'retrieve all marker/term associations supported by a given reference',
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
