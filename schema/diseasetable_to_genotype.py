#!/usr/local/bin/python

import Table

# Contains disease model information for the disease model grid

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'diseasetable_to_genotype'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	diseasetable_genotype_key		int		not null,
	allele_key		int		not null,
	genotype_key		int		not null,
	genotype_seq		int		not null,
	PRIMARY KEY(diseasetable_genotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_key' : 'create index %s on %s (allele_key)',
	'genotype_key' : 'create index %s on %s (genotype_key)',
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'represents the data needed to render disease model information on the allele and genotype detail pages ',
	Table.COLUMN : {
		'diseasetable_genotype_key' : 'unique key identifying this row',
		'allele_key' : 'key for the corresponding allele',
		'genotype_key' : 'key for the corresponding genotype',
		'genotype_seq' : 'sequence for the corresponding genotype',
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
