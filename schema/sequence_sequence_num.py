#!./python

import Table

# contains data definition information for the sequence_sequence_num table

###--- Globals ---###

# name of this database table
tableName = 'sequence_sequence_num'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        sequence_key            int             not null,
        by_length               int             not null,
        by_sequence_type        int             not null,
        by_provider             int             not null,
        PRIMARY KEY(sequence_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'sequence_key' : ('sequence', 'sequence_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the sequence flower, containing pre-computed sorts for quick and easy sorting of sequences',
        Table.COLUMN : {
                'sequence_key' : 'identifies the sequence',
                'by_length' : 'sort by sequence length',
                'by_sequence_type' : 'sort by type of sequence',
                'by_provider' : 'sort by sequence provider',
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
