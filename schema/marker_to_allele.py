#!/usr/local/bin/python

import Table

# contains data definition information for the marker_to_allele table

###--- Globals ---###

# name of this database table
tableName = 'marker_to_allele'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		NOT NULL,
	marker_key	int		NOT NULL,
	allele_key	int 		NOT NULL,
	reference_key	int		NULL,
	qualifier	varchar(80)	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_key' : 'create index %s on %s (allele_key, marker_key)',
	'reference_key' : 'create index %s on %s (reference_key)',
	}

keys = {
	'marker_key' : ('marker', 'marker_key'),
	'allele_key' : ('allele', 'allele_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('marker_key',
	'create index %s on %s (marker_key, allele_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between the marker and allele flowers',
	Table.COLUMN : {
		'unique_key' : 'unique key identifying this association, no other purpose',
		'marker_key' : 'identifies the marker',
		'allele_key' : 'identifies an allele of the marker',
		'reference_key' : 'identifies the reference for this association',
		'qualifier' : 'qualifier describing this association',
		},
	Table.INDEX : {
		'marker_key' : 'clusters the data so all allele associations for a marker are stored together on disk, aiding quick retrieval',
		'allele_key' : 'look up a marker by its allele',
		'reference_key' : 'look up all marker/allele associations for a reference',
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
