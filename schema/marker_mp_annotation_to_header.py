#!./python

import Table

# contains data definition information for the marker_mp_annotation_to_header
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_mp_annotation_to_header'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int     not null,
        mp_annotation_key       int     not null,
        term_key                int     not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'mp_annotation_key' : ('marker_mp_annotation', 'mp_annotation_key'),
        'term_key' : ('term', 'term_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('mp_annotation_key', 'create index %s on %s (mp_annotation_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'maps from an MP annotation for a marker to its MP header terms (as used on the slimgrid)',
        Table.COLUMN : {
                'unique_key' : 'unique key generated to be a primary key for this table (no other purpose)',
                'mp_annotation_key' : 'foreign key to identify which annotation',
                'term_key' : 'foreign key to identify a header term for the annotation',
                },
        Table.INDEX : {
                'mp_annotation_key' : 'clustered index to ensure all rows for an annotation are stored together',
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
