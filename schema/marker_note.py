#!/usr/local/bin/python

import Table

# contains data definition information for the marker_note table

###--- Globals ---###

# name of this database table
tableName = 'marker_note'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	NOT NULL,
	marker_key	int	NOT NULL,
	note_type	varchar(255)	NOT NULL,
	note		varchar	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'marker_key' : ('marker', 'marker_key'), }

# index used to cluster data in the table
clusteredIndex = ('marker_key',
	'create index %s on %s (marker_key, note_type)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the marker flower, containing various types of textual notes for markers',
	Table.COLUMN : {
		'unique_key' : 'unique key identifying this record, no other purpose',
		'marker_key' : 'identifies the marker',
		'note_type' : 'type of note',
		'note' : 'text of the note itself',
		},
	Table.INDEX : {
		'marker_key' : 'clusters the data so that all notes for a marker reside near each other on disk, to aid quick retrieval',
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
