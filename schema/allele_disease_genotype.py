#!/usr/local/bin/python

import Table

# contains data definition information for the allele_disease_genotype table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_disease_genotype'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	disease_genotype_key	int	not null,
	allele_genotype_key	int	not null,
	allele_disease_key	int	not null,
	sequence_num		int	not null,
	PRIMARY KEY(disease_genotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_disease_key' : 'create index %s on %s (allele_disease_key)',
	}

keys = {
	'allele_disease_key' : ('allele_disease', 'allele_disease_key'),
	'allele_genotype_key' : ('allele_to_genotype', 'allele_genotype_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('allele_genotype_key',
	'create index %s on %s (allele_genotype_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the allele flower, identifying which allele/disease records correspond to which allele/genotype pairs',
	Table.COLUMN : {
		'disease_genotype_key' : 'unique identifier for this record, no other significance',
		'allele_genotype_key' : 'identifies the allele/genotype pair',
		'allele_disease_key' : 'identifies the allele/disease pair',
		'sequence_num' : 'orders the allele/disease pair for each allele/genotype',
		},
	Table.INDEX : {
		'allele_genotype_key' : 'clusters the data so all records for an allele/genotype pair are nearby each other on disk, to aid quick access',
		'allele_disease_key' : 'lookup by allele/disease',
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
