#!./python

import Table

# contains data definition information for the genotype_sequence_num table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        genotype_key            int     not null,
        by_alleles              int     not null,
        by_hdp_rules            int     not null,
        PRIMARY KEY(genotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'genotype_key' : ('genotype', 'genotype_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the genotype flower, containing pre-computed sorts for genotypes',
        Table.COLUMN : {
                'genotype_key' : 'identifies the genotype',
                'by_alleles' : 'pre-computed to sort by mutant alleles in the genotype',
                'by_hdp_rules' : 'pre-computed to sort by the hdp-rules (pair state)',
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
