#!./python

import Table

# contains data definition information for the reference_sequence_num table

###--- Globals ---###

# name of this database table
tableName = 'reference_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        reference_key   int             not null,
        by_date         int             not null,
        by_author       int             not null,
        by_primary_id   int             not null,
        by_title        int             not null,
        PRIMARY KEY(reference_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'reference_key' : ('reference', 'reference_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for reference flower, containing pre-computed sorts for references',
        Table.COLUMN : {
                'reference_key' : 'identifes the reference',
                'by_date' : 'integer value for ordering references by date',
                'by_author' : 'integer value for ordering references by author',
                'by_primary_id' : 'integer value for ordering references by ID',
                'by_title' : 'integer value for ordering references by title',
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
