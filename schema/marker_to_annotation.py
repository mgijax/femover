#!./python

import Table

# contains data definition information for the marker_to_annotation table

###--- Globals ---###

# name of this database table
tableName = 'marker_to_annotation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             NOT NULL,
        marker_key      int             NOT NULL,
        annotation_key  int             NOT NULL,
        annotation_type text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'annotation_key' : 'create index %s on %s (annotation_key, marker_key)',
        }

keys = {
        'marker_key' : ('marker', 'marker_key'),
        'annotation_key' : ('annotation', 'annotation_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('marker_key',
        'create index %s on %s (marker_key, annotation_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between the marker and annotation flowers, connecting a marker with its annotations',
        Table.COLUMN : {
                'unique_key' : 'unique key for this association, no other purpose',
                'marker_key' : 'identifies the marker',
                'annotation_key' : 'identifies an annotation for this marker',
                'annotation_type' : 'type of annotation',
                },
        Table.INDEX : {
                'marker_key' : 'clusters data so all annotation associations for a marker are near each other on disk, aiding quick retrieval',
                'annotation_key' : 'look up the marker for a given annotation',
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
