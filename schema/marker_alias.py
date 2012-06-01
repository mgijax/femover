#!/usr/local/bin/python

import Table

# contains data definition information for the marker_alias table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_alias'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	marker_key		int		not null,
	alias_key		int		not null,
	alias_symbol		varchar(50)	null,
	alias_id		varchar(30)	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
	'marker_key' : ('marker', 'marker_key'),
	'alias_key' : ('marker', 'marker_key')
}

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the marker flower, containing data about markers which are aliases for other markers', 
	Table.COLUMN : {
		'unique_key' : 'unique key for this record, no other purpose',
		'marker_key' : 'identifies the marker which has this alias',
		'alias_key' : 'identifies the marker which is the alias',
		'alias_symbol' : 'symbol of the alias marker, cached for convenience',
		'alias_id' : 'primary ID for the alias marker, cached for convenience',
		},
	Table.INDEX : {
		'marker_key' : 'clusters data so all aliases for a marker are grouped together on disk, to aid efficient retrieval',
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
