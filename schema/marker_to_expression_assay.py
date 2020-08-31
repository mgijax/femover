#!./python

import Table

# contains data definition info for the marker_to_expression_assay table

###--- Globals ---###

# name of this database table
tableName = 'marker_to_expression_assay'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             NOT NULL,
        marker_key      int             NOT NULL,
        assay_key       int             NOT NULL,
        reference_key   int             NOT NULL,
        qualifier       text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'assay_key' : 'create index %s on %s (assay_key, marker_key)',
        'reference_key' : 'create index %s on %s (reference_key)',
        }

keys = {
        'marker_key' : ('marker', 'marker_key'),
        'assay_key' : ('expression_assay', 'assay_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('marker_key',
        'create index %s on %s (marker_key, assay_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between the marker and expression assay flowers',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record; no other significance',
                'marker_key' : 'identifies the marker',
                'assay_key' : 'identifies the assay',
                'reference_key' : 'identifies the reference for the association',
                'qualifier' : 'qualifier describing the association',
                },
        Table.INDEX : {
                'marker_key' : 'clusters the data for quick access to all assays of a marker (they will be together on disk)',
                'assay_key' : 'can work from an assay back to its marker',
                'reference_key' : 'can look for all marker/assay pairs drawn from a particular reference',
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
