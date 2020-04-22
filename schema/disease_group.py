#!./python

import Table

# contains data definition information for the disease_group table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'disease_group'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        disease_group_key       int             not null,
        disease_key             int             not null,
        group_type              text    not null,
        sequence_num            int             not null,
        PRIMARY KEY(disease_group_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = { 'disease_key' : ('disease', 'disease_key') }

# index used to cluster data in the table
clusteredIndex = ('disease_key', 'create index %s on %s (disease_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'defines several groupings of rows for each disease (these are sections of related rows)',
        Table.COLUMN : {
                'disease_group_key' : 'uniquely identifies this disease/group pair',
                'disease_key' : 'foreign key to disease table to identify the disease in question',
                'group_type' : 'specifies the type of grouping',
                'sequence_num' : 'orders the groupings for a given disease',
                },
        Table.INDEX : {
                'disease_key' : 'clustered index, keeps the groups for a given disease together on disk for fast access',
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
