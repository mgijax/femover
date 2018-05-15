#!/usr/local/bin/python

import Table

# contains data definition information for the strain_to_reference table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_to_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		NOT NULL,
	strain_key		int		NOT NULL,
	reference_key	int		NOT NULL,
	qualifier		text	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'reference_key' : 'create index %s on %s (reference_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'strain_key' : ('strain', 'strain_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('strain_key', 'create index %s on %s (strain_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between the strain and reference flowers',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other purpose',
		'strain_key' : 'identifies the strain',
		'reference_key' : 'identifies the reference',
		'qualifier' : 'qualifier describing the association',
		},
	Table.INDEX : {
		'reference_key' : 'look up all strains for a reference',
		'strain_key' : 'clusters data so all references for a strain are stored together on disk, to aid quick retrieval',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
