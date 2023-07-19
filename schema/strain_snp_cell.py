#!./python

import Table

# contains data definition information for the strain_snp_cell table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_snp_cell'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        cell_key                                int             not null,
        row_key                                 int             not null,
        chromosome                              text    not null,
        all_count                               int             not null,
        same_count                              int             not null,
        different_count                 int             not null,
        sequence_num                    int             not null,
        PRIMARY KEY(cell_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'row_key' : ('strain_snp_row', 'row_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('row_key', 'create index %s on %s (row_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains one cell per chromosome for each row in strain_snp_row, has three SNP counts (all, same, diff) for that chromosome for the two strains given in strain_snp_row',
        Table.COLUMN : {
                'cell_key' : 'unique (generated) key for this row; identifies one cell in a SNP table shown on the strain detail page',
                'row_key' : 'identifies what row this cell is part of',
                'chromosome' : 'column header for this cell; identifies the chromosome for these counts',
                'all_count' : 'count of all SNPs on the given chromosome for the two strains specified via row_key',
                'same_count' : 'count of SNPs with same allele call on the given chromosome for the two strains specified via row_key',
                'different_count' : 'count of SNPs with different allele calls on the given chromosome for the two strains specified via row_key',
                'sequence_num' : 'precomputed integer, default sort of cells for each row_key',
                },
        Table.INDEX : {
                'row_key' : 'clustered index, for keeping cells for each SNP table row together on disk',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
