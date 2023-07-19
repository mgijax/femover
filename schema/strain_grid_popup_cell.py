#!./python

import Table

# contains data definition information for the strain_grid_popup_cell table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_grid_popup_cell'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        cell_key                int             not null,
        row_key                 int             not null,
        column_key              int             not null,
        color_level             int             not null,
        value                   int             not null,
        sequence_num    int             not null,
        PRIMARY KEY(cell_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = { 'column_key' : 'create index %s on %s (column_key)' }

# column name -> (related table, column in related table)
keys = {
        'row_key' : ('strain_grid_popup_row', 'row_key'),
        'column_key' : ('strain_grid_popup_column', 'column_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('row_key', 'create index %s on %s (row_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'Each row contains data for one cell of a strain phenotype popup table, identified by its row and column.',
        Table.COLUMN : {
                'cell_key' : 'primary key of this table, uniquely identifying each cell',
                'row_key' : 'identifies the row containing this cell',
                'column_key' : 'identifies the column containing this cell',
                'color_level' : 'identifies a color level for the cell',
                'value' : 'count of annotations for this cell',
                'sequence_num' : 'orders the cells for a table row; assumed to be in sync with sequence_num in strain_grid_popup_column',
                },
        Table.INDEX : {
                'column_key' : 'quickly find the cell(s) for a given column',
                'row_key' : 'clustered index, bringing together all the cell values for a given row for quick access',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
