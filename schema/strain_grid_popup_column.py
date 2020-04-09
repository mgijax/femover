#!./python

import Table

# contains data definition information for the strain_grid_popup_column table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_grid_popup_column'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        column_key              int             not null,
        grid_cell_key   int             not null,
        term                    text    null,
        sequence_num    int             not null,
        PRIMARY KEY(column_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = { 'grid_cell_key' : ('strain_grid_cell', 'grid_cell_key') }

# index used to cluster data in the table
clusteredIndex = ('grid_cell_key', 'create index %s on %s (grid_cell_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains the column headers for the popup table for a strain phenogrid cell, one per row',
        Table.COLUMN : {
                'column_key' : 'unique identifier for this column data row (row is particular to a given term and grid cell)',
                'grid_cell_key' : 'identifies which phenogrid cell was clicked to find this data (on a strain detail page)',
                'term' : 'text of the term that is the column header',
                'sequence_num' : 'sequence number for ordering the column headers to be displayed for a single phenogrid cell',
                },
        Table.INDEX : {
                'grid_cell_key' : 'clustered index, bringing all column headers of a phenogrid cell together for fast access',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
