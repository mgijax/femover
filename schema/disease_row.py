#!./python

import Table

# contains data definition information for the disease_row table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'disease_row'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        disease_row_key         int     not null,
        disease_group_key       int     not null,
        sequence_num            int     not null,
        cluster_key             int     null,
        PRIMARY KEY(disease_row_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'cluster_key' : 'create index %s on %s (cluster_key)',
        }

# column name -> (related table, column in related table)
keys = { 'disease_group_key' : ('disease_group', 'disease_group_key'),
        'cluster_key' : ('homology_cluster', 'cluster_key'), 
        }

# index used to cluster data in the table
clusteredIndex = ('disease_group_key',
        'create index %s on %s (disease_group_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'defines one displayable row in a table for a disease, possibly including multiple markers and optionally tied to a homology cluster',
        Table.COLUMN : {
                'disease_row_key' : 'uniquely identifies this disease row',
                'disease_group_key' : 'foreign key to disease_group to identify which group this row is part of',
                'sequence_num' : 'orders the rows for each disease group',
                'cluster_key' : 'foreign key to homology_cluster to identify the cluster for the markers in this row',
                },
        Table.INDEX : {
                'cluster_key' : 'allows quick lookup of rows for a given homology cluster (not likely often used)',
                'disease_group_key' : 'clustered index to group all rows for a disease group together on disk for fast access',
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
