#!/usr/local/bin/python

import Table

# contains data definition information for the genotype_phenotype_note table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype_phenotype_note'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	genotype_phenotype_key	int		not null,
	note			text		not null,
	jnum_id			varchar(40)	null,
	sequence_num		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'gp_key' : 'create index %s on %s (genotype_phenotype_key, sequence_num)',
	}

keys = { 'genotype_phenotype_key' : ('genotype_phenotype',
	'genotype_phenotype_key') }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
