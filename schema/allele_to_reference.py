#!./python

import Table

# contains data definition information for the alleleToReference table

###--- Globals ---###

# name of this database table
tableName = 'allele_to_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int     NOT NULL,
        allele_key      int     NOT NULL,
        reference_key   int     NOT NULL,
        qualifier       text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'reference_key' : 'create index %s on %s (reference_key, allele_key)',
        }

keys = {
        'allele_key' : ('allele', 'allele_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('allele_key',
        'create index %s on %s (allele_key, reference_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between the allele and reference flowers',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record, no other purpose',
                'allele_key' : 'identifies the allele',
                'reference_key' : 'identifies the reference',
                'qualifier' : 'qualifier describing the association',
                },
        Table.INDEX : {
                'reference_key' : 'look up all alleles for a reference',
                'allele_key' : 'clusters data so all references for an allele are stored together on disk, to aid quick retrieval',
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
