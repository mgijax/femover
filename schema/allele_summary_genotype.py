#!/usr/local/bin/python

import Table

# Contains only genotypes that are included on the allele summary page

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_summary_genotype'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_genotype_key		int		not null,
	allele_key		int		not null,
	genotype_key		int		not null,
	PRIMARY KEY(allele_genotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_key': 'create index %s on %s (allele_key)',
	'genotype_key': 'create index %s on %s (genotype_key)',
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'represents the Non-Normal systems displayed on summary page',
	Table.COLUMN : {
		'allele_genotype_key' : 'unique key identifying this row',
		'allele_key' : 'key for the allele',
		'genotype_key' : 'key for the genotype',
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
