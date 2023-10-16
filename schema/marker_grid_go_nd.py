#!./python

import Table

# contains data definition information for the marker_grid_go_nd table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_grid_go_nd'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        go_nd_key             int     not null,
        marker_key            int     not null,
        go_dag                text    not null,
        is_nd                 bool     not null,
        PRIMARY KEY(go_nd_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = { }

# column name -> (related table, column in related table)
keys = {
        'marker_key' : ('marker', 'marker_key'),
}
# index used to cluster data in the table
clusteredIndex = ('marker_key',
        'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'marker_grid_go_nd',
        Table.COLUMN : {
                'go_nd_key' : 'unique key for this table',
                'marker_key' : 'marker this row does with',
                'go_dag' : 'which GO dag (M, C, or P)',
                'is_nd' : 'if 1, this dag has ND annotation and no other annotations',
                },
        Table.INDEX : {
                'marker_key' : 'get all go_nd records for a marker',
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
