#!./python

import Table

# contains data definition information for the accession_logical_db table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'accession_logical_db'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        logical_db_key          int             not null,
        logical_db              text    not null,
        PRIMARY KEY(logical_db_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'logical_db' : 'create index %s on %s (logical_db)',
        }

comments = {
        Table.TABLE : 'petal table for the accession flower; contains names for logical databases.  A logical database identifies the source of a given accession ID.',
        Table.COLUMN : {
            'logical_db_key' : 'unique key identifying a logical database',
            'logical_db' : 'name of the logical database',
                },
        Table.INDEX : {
            'logical_db' : 'for searching by the logical database name',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
