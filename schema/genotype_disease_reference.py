#!/usr/local/bin/python

import Table

# contains data definition info for the genotype_disease_reference table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype_disease_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	genotype_disease_key	int		not null,
	reference_key		int		not null,
	jnum_id			varchar(40)	not null,
	sequence_num		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
	'genotype_disease_key' : ('genotype_disease', 'genotype_disease_key'),
	'reference_key' : ('reference', 'reference_key')
	}

# index used to cluster data in the table
clusteredIndex = ('genotype_disease_key',
	'create index %s on %s (genotype_disease_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the genotype flower, containing references for annotations of diseases to allele/genotype pairs',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other significance',
		'genotype_disease_key' : 'identifies the disease annotation',
		'reference_key' : 'reference supporting the annotation',
		'jnum_id' : 'J: number ID for the reference, cached for convenience',
		'sequence_num' : 'used to order the references for a given annotation',
		},
	Table.INDEX : {
		'genotype_disease_key' : 'clusters data so all the references for a disease annotation are stored together on disk, to aid quick retrieval',
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
