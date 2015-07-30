#!/usr/local/bin/python

import Table

# contains data definition information for the marker_to_reference table

###--- Globals ---###

# name of this database table
tableName = 'marker_to_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	NOT NULL,
	marker_key	int	NOT NULL,
	reference_key	int	NOT NULL,
	is_strain_specific	int	NOT NULL,
	qualifier	text	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'reference_key' : 'create index %s on %s (reference_key, marker_key)',
	}

keys = {
	'marker_key' : ('marker', 'marker_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('marker_key',
	'create index %s on %s (marker_key, reference_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between markers and references',
	Table.COLUMN : {
		'unique_key' : 'unique key identifying this record, no other purpose',
		'marker_key' : 'identifies the marker',
		'reference_key' : 'identifies the reference',
		'is_strain_specific' : '1 if this is for a strain-specific marker/reference association, 0 if not',
		'qualifier' : 'qualifier describing this association:  earliest, latest, private',
		},
	Table.INDEX : {
		'marker_key' : 'clusters data so that all reference associations for a marker will be near each other on disk, aiding quick access',
		'reference_key' : 'look up all markers for a given reference',
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
