#!./python

import Table

# contains data definition information for the strain_snp_row table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_snp_row'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        row_key                                 int             not null,
        strain_key                              int             not null,
        comparison_strain_key   int             not null,
        comparison_strain_name  text    null,
        comparison_strain_id    text    null,
        sequence_num                    int             not null,
        PRIMARY KEY(row_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'comparison_strain_key' : 'create index %s on %s (comparison_strain_key)',
        'comparison_strain_id' : 'create index %s on %s (comparison_strain_id)',
        }

# column name -> (related table, column in related table)
keys = {
        'strain_key' : ('strain', 'strain_key'),
        'comparison_strain_key' : ('strain', 'strain_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('strain_key', 'create index %s on %s (strain_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table in strain flower; contains rows for the SNP table for each strain having SNPs',
        Table.COLUMN : {
                'row_key' : 'unique (generated) key for this table; identifies on SNP table row',
                'strain_key' : 'identifies the reference strain (the strain for the detail page the user is viewing)',
                'comparison_strain_key' : 'identifies the comparison strain for this row',
                'comparison_strain_name' : 'name of comparison strain, cached for convenience',
                'comparison_strain_id' : 'primary ID of comparison strain, cached for convenience',
                'sequence_num' : 'precomputed integer, default sort of rows for each strain_key',
                },
        Table.INDEX : {
                'strain_key' : 'clustered index, for keeping rows together on disk for each strain detail page',
                'comparison_strain_key' : 'facilitates joins to strain table for comparison strain',
                'comparison_strain_id' : 'facilitates lookups for by primary ID of comparison strain',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
