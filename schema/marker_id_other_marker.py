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
indexes = {
	'marker_id_key' : 'create index %s on %s (marker_id_key)',
	}

keys = {
	'marker_id_key' : ('marker_id', 'unique_key'),
	'marker_key' : ('marker', 'marker_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
