#!/usr/local/bin/python

import Table

# contains data definition info for the genotype_phenotype_reference table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype_phenotype_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	genotype_phenotype_key	int		not null,
	reference_key		int		not null,
	jnum_id			varchar(40)	not null,
	sequence_num		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'genotype_phenotype_key' : ('genotype_phenotype',
	'genotype_phenotype_key'),
	'reference_key' : ('reference', 'reference_key')
	}

# index used to cluster data in the table
clusteredIndex = ('gp_key',
	'create index %s on %s (genotype_phenotype_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the genotype flower, containing references supporting annotations of phenotypes to genotypes',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other significance',
		'genotype_phenotype_key' : 'identifies the phenotype annotation',
		'reference_key' : 'identifies a reference supporting the annotation',
		'jnum_id' : 'J: number ID for the reference, cached for convenience',
		'sequence_num' : 'for ordering the references of an annotation',
		},
	Table.INDEX : {
		'gp_key' : 'clusters data so that all references for an annotation are together on disk, for quick access',
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
