#!/usr/local/bin/python

import Table

# contains data definition information for the genotype_disease_footnote table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype_disease_footnote'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	genotype_disease_key	int		not null,
	number			int		not null,
	note			varchar(255)	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'genotype_disease' : 'create index %s on %s (genotype_disease_key, number)',
	}

keys = { 'genotype_disease_key' : ('genotype_disease', 'genotype_disease_key')}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the genotype flower, containing footnotes for annotations of diseases to allele/genotype pairs',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other significance',
		'genotype_disease_key' : 'identifies the disease annotation which has this footnote',
		'number' : 'number of the footnote',
		'note' : 'the footnote itself',
		},
	Table.INDEX : {
		'genotype_disease' : 'quick access to any footnote(s) for a disease annotation',
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
