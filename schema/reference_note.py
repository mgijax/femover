#!/usr/local/bin/python

import Table

# contains data definition information for the reference_note table

###--- Globals ---###

# name of this database table
tableName = 'reference_note'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	NOT NULL,
	reference_key	int	NOT NULL,
	note_type	varchar(255)	NOT NULL,
	note		varchar(64000)	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'reference_key' : ('reference', 'reference_key'), }

# index used to cluster data in the table
clusteredIndex = ('reference_key',
	'create index %s on %s (reference_key, note_type)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the reference flower, containing notes associated with references',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this reference/note pair',
		'reference_key' : 'identifies the reference',
		'note_type' : 'type of note',
		'note' : 'the note itself',
		},
	Table.INDEX : {
		'reference_key' : 'clusters data together so a given reference has its notes close together on disk',
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
