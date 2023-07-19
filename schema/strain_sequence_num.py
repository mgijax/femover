#!./python

import Table

# contains data definition information for the strain_sequence_num table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_sequence_num'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        strain_key              int             NOT NULL,
        by_strain               int             NOT NULL,
        PRIMARY KEY(strain_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'by_strain' : 'create index %s on %s (by_strain)',
        }

# column name -> (related table, column in related table)
keys = {
        'strain_key' : ('strain', 'strain_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'pre-computed sequence numbers for ordering strains',
        Table.COLUMN : {
                'strain_key' : 'identifies the strain (FK to strain table)',
                'by_strain' : 'orders the strain by its name',
                },
        Table.INDEX : {
                'by_strain' : 'easy lookup of the strains ordered between m and n',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
