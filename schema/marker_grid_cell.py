#!./python

import Table

# contains data definition information for the marker_grid_cell table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_grid_cell'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        grid_cell_key   int     not null,
        marker_key      int     not null,
        heading_key     int     not null,
        color_level     int     not null,
        value           int     not null,
        sequence_num    int     not null,
        PRIMARY KEY(grid_cell_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'heading_key' : 'create index %s on %s (heading_key)',
        }

# column name -> (related table, column in related table)
keys = { 'marker_key' : ('marker', 'marker_key'),
        'heading_key' : ('marker_grid_heading', 'heading_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('marker_key',
        'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'marker_grid_cell',
        Table.COLUMN : {
                'grid_cell_key' : 'unique key for this table (no other purpose)',
                'marker_key' : 'identifies the marker (also used to cluster the rows in the table for efficient access)',
                'heading_key' : 'identifies the heading for this cell',
                'color_level' : 'identifies a color level for the cell',
                'value' : 'count of annotations for this cell',
                'sequence_num' : 'orders the cells for a marker; assumed to be in sync with sequence_num in marker_grid_heading',
                },
        Table.INDEX : {
                'heading_key' : 'quick access for a specific header term',
                'marker_key' : 'clustered index, to group records by marker for fast access',
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
