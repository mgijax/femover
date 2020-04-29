#!./python

import Table

# contains data definition information for the expression_result_to_imagepane
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_result_to_imagepane'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int     not null,
        result_key      int     not null,
        imagepane_key   int     not null,
        sequence_num    int     not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
clusteredIndex = ('result_key', 'create index %s on %s (result_key)')

indexes = {
        'imagepane_key' : 'create index %s on %s (imagepane_key)',
        }

# column name -> (related table, column in related table)
keys = {
        'result_key' : ('expression_result_summary', 'result_key'),
        'imagepane_key' : ('expression_imagepane', 'imagepane_key'),
        }

comments = {
        Table.TABLE : 'join table; relates expression results to their associated image panes',
        Table.COLUMN : {
                'unique_key' : 'integer key; needed for Hibernate to have object identity, no other purpose',
                'result_key' : 'foreign key to expression_result_summary; specifies which expression result is associated with the image pane',
                'imagepane_key' : 'foreign key to expression_imagepane; specifies which image pane is associated with the expression result',
                'sequence_num' : 'used to order the image panes for a given expression result',
                },
        Table.INDEX : {
                'result_key' : 'clustered so image panes for a given expression result will be stored near each other',
                'imagepane_key' : 'for quick access to all expression results for a given image pane',
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
