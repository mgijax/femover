#!/usr/local/bin/python

import Table

# contains data definition information for the reference_to_sequence table

###--- Globals ---###

# name of this database table
tableName = 'reference_to_sequence'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		NOT NULL,
	sequence_key	int 		NOT NULL,
	reference_key	int		NOT NULL,
	qualifier	text	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'sequence_key' : 'create index %s on %s (sequence_key)',
	}

keys = {
	'reference_key' : ('reference', 'reference_key'),
	'sequence_key' : ('sequence', 'sequence_key')
	}

# index used to cluster data in the table
clusteredIndex = ('reference_key', 'create index %s on %s (reference_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between references and sequences',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifes this record, no other purpose',
		'sequence_key' : 'identifies the sequence',
		'reference_key' : 'identifies the reference',
		'qualifier' : 'qualifier describing the association',
		},
	Table.INDEX : {
		'sequence_key' : 'allows lookup of references for a given sequence',
		'reference_key' : 'clusters data so sequences for a reference will be stored nearby on disk, for quick access to sequences by reference',
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
