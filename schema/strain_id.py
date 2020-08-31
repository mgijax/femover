#!./python

import Table

# contains data definition information for the strain_id table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_id'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int     NOT NULL,
        strain_key              int     NOT NULL,
        logical_db              text NULL,
        acc_id                  text NULL,
        preferred               int     NOT NULL,
        private                 int     NOT NULL,
        sequence_num    int     NOT NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'acc_id' : 'create index %s on %s (acc_id)',
        }

# column name -> (related table, column in related table)
keys = {
        'strain_key' : ('strain', 'strain_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('strain_key', 'create index %s on %s (strain_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table in the strain flower, containing various accession IDs assigned to strains',
        Table.COLUMN : {
                'unique_key' : 'unique key for this strain/ID pair',
                'strain_key' : 'identifies the strain',
                'logical_db' : 'logical database assigning the ID',
                'acc_id' : 'accession ID',
                'preferred' : '1 if this is the preferred ID for this strain by this logical database, 0 if not',
                'private' : '1 if this ID should be considered private, 0 if it can be displayed',
                'sequence_num' : 'orders IDs for a strain',
                },
        Table.INDEX : {
                'strain_key' : 'clusters data so all IDs for a strain are stored near each other on disk, to aid quick retrieval',
                'acc_id' : 'look up a strain by its ID',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
