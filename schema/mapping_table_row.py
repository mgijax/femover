#!./python

import Table

# contains data definition information for the mapping_table_ro table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_table_row'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        row_key                         int             not null,
        mapping_table_key       int             not null,
        is_header                       int             not null,
        sequence_num            int             not null,
        PRIMARY KEY(row_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'mapping_table_key' : ('mapping_table', 'mapping_table_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('mapping_table_key', 'create index %s on %s (mapping_table_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'Each record defines one row (header or data) in a table of a mapping experiment.',
        Table.COLUMN : {
                'row_key' : 'generated primary key for the table; no other significance',
                'mapping_table_key' : 'foreign key to mapping_table, identifying the table containing this row',
                'is_header' : 'flag to indicate if this is a header row (1) or not (0)',
                'sequence_num' : 'used to order the rows of each table',
                },
        Table.INDEX : {
                'mapping_table_key' : 'clustered index to keep all rows of a mapping experiment table together on disk for quick access',
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
