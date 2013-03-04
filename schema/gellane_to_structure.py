#!/usr/local/bin/python

import Table

# contains data definition information for the gellane_to_structure table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'gelllane_to_structure'

# PgSQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	gellane_key		int		not null,
	mgd_structure_key		int		not null,
	printname		text		not null,
	structure_seq		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'gellane_key' : 'create index %s on %s (gellane_key)',
	'mgd_structure_key' : 'create index %s on %s (mgd_structure_key)',
	}

keys = {
	'gellane_key' : ('gellane', 'gellane_key'),
	}

clusteredIndex=None
# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'Contains information about structures for gellane objects',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this row',
		'gellane_key' : 'unique identifier for this lane, same as _gellane_key in mgd',
		'mgd_structure_key' : 'unique identifier for this structure, same as _Structure_key in mgd',
		'printname' : 'stage+structure',
		'structure_seq' : 'seq for the structures (if needed)',
		},
	Table.INDEX : {
		'gellane_key' : 'foreign key to gellane',
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
