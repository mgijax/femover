#!/usr/local/bin/python

import Table

# contains data definition information for the marker_orthology table

###--- Globals ---###

# name of this database table
tableName = 'marker_orthology'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	NOT NULL,
	mouse_marker_key	int	NOT NULL,
	other_marker_key	int	NOT NULL,
	other_symbol		varchar(50)	NULL,
	other_organism		varchar(50)	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'mouse_marker' : 'create index %s on %s (mouse_marker_key)',
	'other_marker' : 'create index %s on %s (other_marker_key)',
	'other_organism' : 'create index %s on %s (other_organism)',
	}

keys = {
	'mouse_marker_key' : ('marker', 'marker_key'),
	'other_marker_key' : ('marker', 'marker_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
