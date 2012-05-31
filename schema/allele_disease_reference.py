#!/usr/local/bin/python

import Table

# contains data definition info for the allele_disease_reference table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_disease_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	disease_genotype_key	int		not null,
	reference_key		int		not null,
	jnum_id			varchar(40)	not null,
	sequence_num		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
	'disease_genotype_key' : ('allele_disease_genotype',
		'disease_genotype_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('disease_genotype_key',
	'create index %s on %s (disease_genotype_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the allele flower, containing references for each genotype/disease association',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this ',
		'disease_genotype_key' : 'identifies the genotype/disease pair',
		'reference_key' : 'identifies a reference for the genotype/disease association',
		'jnum_id' : 'J: number ID of the reference, cached for convenience',
		'sequence_num' : 'orders the references for each genotype/disease pair',
		},
	Table.INDEX : {
		'disease_genotype_key' : 'clusters the data so all references for a genotype/disease pair are near each other on disk, to speed retrieval',
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
