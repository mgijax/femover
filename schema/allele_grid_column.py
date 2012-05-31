#!/usr/local/bin/python

import Table

# contains data definition information for the allele_grid_column table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_grid_column'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	grid_column_key		int		not null,
	allele_key		int		not null,
	allele_genotype_key	int		not null,
	genotype_type		varchar(2)	not null,
	genotype_designation	varchar(6)	not null,
	sequence_num		int		not null,
	PRIMARY KEY(grid_column_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
	'allele_key' : ('allele', 'allele_key'),
	'allele_genotype_key' : ('allele_to_genotype', 'allele_genotype_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('allele_key',
	'create index %s on %s (allele_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the allele flower, identifying the columns for the phenotype grid for each allele',
	Table.COLUMN : {
		'grid_column_key' : 'unique identifier for this record, no other purpose',
		'allele_key' : 'identifies the allele',
		'allele_genotype_key' : 'identifies the allele/genotype pair represented by this column',
		'genotype_type' : 'name of the type of genotype',
		'genotype_designation' : 'abbreviation for identifying this genotype for this allele',
		'sequence_num' : 'orders the columns for an allele',
		},
	Table.INDEX : {
		'allele_key' : 'clusters the data so records for a given allele will be stored together on disk to aid quick retrieval',
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
