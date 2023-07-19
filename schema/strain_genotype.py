#!./python

import Table

# contains data definition information for the strain_genotype table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_genotype'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        strain_genotype_key             int             NOT NULL,
        strain_key                              int             NOT NULL,
        genotype_key                    int             NOT NULL,
        genotype_id                             text    NULL,
        abbreviation                    text    NULL,
        has_disease                             int             NOT NULL,
        sequence_num                    int             NOT NULL,
        PRIMARY KEY(strain_genotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'genotype_key' : 'create index %s on %s (genotype_key)',
        'genotype_id' : 'create index %s on %s (genotype_id)',
        }

# column name -> (related table, column in related table)
keys = {
        'strain_key' : ('strain', 'strain_key'),
        'genotype_key' : ('genotype', 'genotype_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('strain_key', 'create index %s on %s (strain_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for strain flower, including only those genotypes where the genotype IS the strain',
        Table.COLUMN : {
                'strain_genotype_key' : 'generated key to uniquely identify this strain/genotype pair',
                'strain_key' : 'identifies the strain',
                'genotype_key' : 'identifies the genotype, FK to genotype table',
                'genotype_id' : 'primary accession ID of the genotype',
                'has_disease' : 'flag (0/1) to indicate if this disease/strain pair has disease annotations',
                'abbreviation' : 'label to use for this genotype on the strain detail page',
                'sequence_num' : 'precomputed integer for ordering genotypes of a strain',
                },
        Table.INDEX : {
                'genotype_key' : 'look up all strains for a genotype by key',
                'genotype_id' : 'look up all strains for a genotype by ID',
                'strain_key' : 'clusters data so all genotypes for a strain are stored together on disk, to aid quick retrieval',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
