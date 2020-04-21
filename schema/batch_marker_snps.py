#!./python

import Table

# contains data definition information for the batch_marker_snps table

###--- Globals ---###

# name of this database table
tableName = 'batch_marker_snps'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             not null,
        marker_key      int             not null,
        snp_id          text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'marker_key' : ('marker', 'marker_key') } 

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the marker flower, containing basic data for SNPs associated with each marker (to aid efficiency of batch query)',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this marker/SNP pair',
                'marker_key' : 'identifies the marker',
                'snp_id' : 'consensus SNP ID',
                },
        Table.INDEX : {
                'marker_key' : 'clusters data together by marker key, so all SNPs for a marker will be together on disk (for speed of efficiency)',
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
