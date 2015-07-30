#!/usr/local/bin/python

import Table

# contains data definition information for the genotype_to_annotation table

###--- Globals ---###

# name of this database table
tableName = 'genotype_to_annotation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		NOT NULL,
	genotype_key	int		NOT NULL,
	annotation_key	int 		NOT NULL,
	reference_key	int		NULL,
	qualifier	text	NULL,
	annotation_type	text	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'annotation_key' : 'create index %s on %s (annotation_key, genotype_key)',
	}

keys = {
	'genotype_key' : ('genotype', 'genotype_key'),
	'annotation_key' : ('annotation', 'annotation_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('genotype_key',
	'create index %s on %s (genotype_key, annotation_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between the genotype and annotation flowers, connecting a genotype with its annotations',
	Table.COLUMN : {
		'unique_key' : 'unique key for this association, no other purpose',
		'genotype_key' : 'identifies the genotype',
		'annotation_key' : 'identifies an annotation for this genotype',
		'reference_key' : 'identifies the reference asserting this annotation',
		'qualifier' : 'qualifier describing this association',
		'annotation_type' : 'type of annotation',
		},
	Table.INDEX : {
		'genotype_key' : 'clusters data so all annotation associations for a genotype are near each other on disk, aiding quick retrieval',
		'annotation_key' : 'look up the genotype for a given annotation',
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
