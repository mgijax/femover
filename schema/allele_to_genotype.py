#!/usr/local/bin/python

import Table

# contains data definition information for the allele_to_genotype table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_to_genotype'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_genotype_key	int		not null,
	allele_key		int		not null,
	genotype_key		int		not null,
	qualifier		varchar(80)	null,
	genotype_type		varchar(2)	null,
	genotype_designation	varchar(7)	null,
	has_phenotype_data	int		not null,
	is_disease_model	int		not null,
	sequence_num		int		not null,
	PRIMARY KEY(allele_genotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_key' : 'create index %s on %s (allele_key, sequence_num)',
	'genotype_key' : 'create index %s on %s (genotype_key)',
	}

keys = {
	'allele_key' : ('allele', 'allele_key'),
	'genotype_key' : ('genotype', 'genotype_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
