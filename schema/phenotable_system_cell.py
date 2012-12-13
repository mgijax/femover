#!/usr/local/bin/python

import Table

# contains MP term information for the pheno summary table 

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'phenotable_system_cell'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	phenotable_system_cell_key		int		not null,
	phenotable_system_key		int		not null,
	call		varchar(10)	not null,
	sex		varchar(10)	not null,
	genotype_id		varchar(255)	not null,
	cell_seq			int	null,
	genotype_seq			int	null,
	PRIMARY KEY(phenotable_system_cell_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'system_key' : 'create index %s on %s (phenotable_system_key)',
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'represents the data needed to render phenotype information on the allele and genotype detail pages ',
	Table.COLUMN : {
		'phenotable_system_cell_key' : 'unique key identifying this row',
		'phenotable_system_key' : 'key for the corresponding system',
		'call' : 'Y, N, or NA',
		'sex' : 'M, F, or NA',
		'genotype_id' : 'genotype MGIID for linking',
		'cell_seq' : 'sort order by genotype column',
		'genotype_seq' : 'genotype counter',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
