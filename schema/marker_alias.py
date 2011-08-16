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
indexes = {
	'marker_key' : 'create index %s on %s (marker_key)',
	}

keys = {
	'marker_key' : ('marker', 'marker_key'),
	'alias_key' : ('marker', 'marker_key')
	}
# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
