#!./python

import Table

# contains data definition information for the antibody_sequence_num table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'antibody_sequence_num'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        antibody_key            int             NOT NULL,
        by_name                 int             NOT NULL,
        by_gene                 int             NOT NULL,
        by_ref_count            int             NOT NULL,
        PRIMARY KEY(antibody_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'antibody_key' : ('antibody', 'antibody_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains pre-computed sequence numbers for ordering antibodies',
        Table.COLUMN : {
                'antibody_key' : 'identifies the antibody',
                'by_name' : 'smart-alpha ordering by antibody name, then type, then primary ID',
                'by_gene' : 'smart-alpha ordering by gene symbol, then antibody name, then primary ID',
                'by_ref_count' : 'smart-alpha ordering by #references, then antibody name, then primary ID',
                },
        Table.INDEX : {},
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
                clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
