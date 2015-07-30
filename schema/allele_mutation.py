#!/usr/local/bin/python

import Table

# contains data definition information for the allele_mutation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_mutation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		not null,
	allele_key	int		not null,
	mutation	text	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

# index used to cluster data in the table
clusteredIndex = ('allele_key', 'create index %s on %s (allele_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the allele flower, containing mutations for each allele',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this row, no other purpose',
		'allele_key' : 'identifies the allele',
		'mutation' : 'one mutation for the allele',
		},
	Table.INDEX : {
		'allele_key' : 'clusters data so that all mutations for an allele are stored together on disk, to aid quick retrieval',
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
