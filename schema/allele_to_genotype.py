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
	qualifier		text	null,
	genotype_type		text	null,
	genotype_designation	text	null,
	has_phenotype_data	int		not null,
	is_disease_model	int		not null,
	sequence_num		int		not null,
	PRIMARY KEY(allele_genotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'genotype_key' : 'create index %s on %s (genotype_key)',
	}

keys = {
	'allele_key' : ('allele', 'allele_key'),
	'genotype_key' : ('genotype', 'genotype_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('allele_key',
	'create index %s on %s (allele_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between the allele and genotype flowers',
	Table.COLUMN : {
		'allele_genotype_key' : 'unique identifier for this allele/genotype association',
		'allele_key' : 'identifies the allele',
		'genotype_key' : 'identifies the genotype having this allele',
		'qualifier' : 'qualifier describing this association',
		'genotype_type' : 'type of genotype',
		'genotype_designation' : 'abbreviation used to reference this genotype for this allele',
		'has_phenotype_data' : '1 if this genotype has phenotype data, 0 if not',
		'is_disease_model' : '1 if this genotype is a mouse model of a human disease, 0 if not',
		'sequence_num' : 'used to order genotypes for each allele',
		},
	Table.INDEX : {
		'allele_key' : 'clusters data so all genotype associations for an allele are stored together on disk, to aid quick retrieval',
		'genotype_key' : 'look up alleles for a genotype',
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
