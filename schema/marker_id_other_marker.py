#!/usr/local/bin/python

import Table

# contains data definition information for the marker_id_other_marker table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_id_other_marker'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	marker_id_key		int		not null,
	marker_key		int		not null,
	symbol			varchar(50)	null,
	primary_id		varchar(30)	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
	'marker_id_key' : ('marker_id', 'unique_key'),
	'marker_key' : ('marker', 'marker_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('marker_id_key', 'create index %s on %s (marker_id_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal in the marker flower, containing data about other markers which share the same ID with a given marker',
	Table.COLUMN : {
		'unique_key' : 'unique key identifying this record, no other purpose',
		'marker_id_key' : 'identifies the marker/ID pair which has other markers sharing that ID (references marker_id.unique_key)',
		'marker_key' : 'identifies the other marker which shares the ID',
		'symbol' : 'symbol of the other marker, cached for convenience',
		'primary_id' : 'primary ID of the other marker, cached for convenience',
		},
	Table.INDEX : {
		'marker_id_key' : 'clusters data so that all other markers for a given marker/ID pair can be returned quickly',
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
