#!./python

import Table

# contains data definition information for the alleleID table

###--- Globals ---###

# name of this database table
tableName = 'allele_id'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int     NOT NULL,
        allele_key      int     NOT NULL,
        logical_db      text NULL,
        acc_id          text NULL,
        preferred       int     NOT NULL,
        private         int     NOT NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'acc_id' : 'create index %s on %s (acc_id)',
        }

keys = { 'allele_key' : ('allele', 'allele_key') }

# index used to cluster data in the table
clusteredIndex = ('allele_key', 'create index %s on %s (allele_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the allele flower, containing accession IDs assigned to the various alleles',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record, no other significance',
                'allele_key' : 'identifies the allele',
                'logical_db' : 'logical database assigning the ID',
                'acc_id' : 'accession ID',
                'preferred' : '1 if this is the preferred ID for this logical database for this allele, 0 if not',
                'private' : '1 if this ID should be considered private, 0 if it can be displayed',
                },
        Table.INDEX : {
                'allele_key' : 'clusters the data so all IDs for an allele are stored together on disk, to aid quick retrieval',
                'acc_id' : 'look up alleles by ID, case sensitive',
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
