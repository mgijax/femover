#!./python

import Table

# contains data definition information for the recombinase_other_allele table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'recombinase_other_allele'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        allele_system_key       int             not null,
        other_allele_key        int             not null,
        other_allele_id         text    null,
        other_allele_symbol     text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
        'allele_system_key' : ('recombinase_allele_system',
                'allele_system_key'),
        'other_allele_key' : ('allele', 'allele_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('allele_system_key',
        'create index %s on %s (allele_system_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the recombinase flower, containing (for each allele/system pair) the other alleles with recombinase activity for the same system',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this record',
                'allele_system_key' : 'identifies the allele/system pair with which this record is associated',
                'other_allele_key' : 'key of another allele',
                'other_allele_id' : 'primary ID of the other allele, cached here for convenience',
                'other_allele_symbol' : 'symbol of the other allele, cached here for convenience',
                },
        Table.INDEX : {
                'allele_system_key' : 'clusters data so that all other alleles for an allele/system pair are grouped together on disk to aid efficiency',
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
