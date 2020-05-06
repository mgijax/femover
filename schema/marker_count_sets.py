#!./python

import Table

# contains data definition information for the marker_count_sets table

###--- Globals ---###

# name of this database table
tableName = 'marker_count_sets'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             not null,
        marker_key      int             not null,
        set_type        text    not null,
        count_type      text    not null,
        count           int             not null,
        sequence_num    int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'marker_key' : ('marker', 'marker_key') }

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table in the marker flower, containing data for various count sets for each marker.  A count set is a named group of related counts, where each of those individual counts in the group is also named.',
        Table.COLUMN : {
                'unique_key' : 'unique key for this record, no other purpose',
                'marker_key' : 'identifies the marker',
                'set_type' : 'name of the count set (the group of related counts)',
                'count_type' : 'name of this individual count (one member in the count set)',
                'count' : 'the count itself',
                'sequence_num' : 'orders count set items for each marker',
                },
        Table.INDEX : {
                'marker_key' : 'clusters data so that all count set members for a marker are stored together on disk, aiding fast retrieval',
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
