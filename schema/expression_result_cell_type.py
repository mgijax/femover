#!./python

import Table

# contains data definition information for the
# expression_result_cell_type table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_result_cell_type'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int     not null,
        result_key              int     not null,
        cell_type               text    not null,
        sequence_num            int     not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {'result_key' : ('expression_result_summary', 'result_key')}

# index used to cluster data in the table
clusteredIndex = ('result_key', 'create index %s on %s (result_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'Maps from an expression result key to its cell types (can be multiple rows per result)',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies a record',
                'result_key' : 'key for the expression result',
                'cell_type' : 'one of the cell types for this expression result',
                'sequence_num' : 'for ordering the cell types of each result'
                },
        Table.INDEX : {
                'result_key' : 'clustered index to group terms by result key for fast retrieval',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
