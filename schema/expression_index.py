#!/usr/local/bin/python

import Table

# contains data definition information for the expression_index table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_index'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	index_key		int		not null,
	reference_key		int		not null,
	jnum_id			varchar(30)	null,
	marker_key		int		not null,
	marker_symbol		varchar(50)	null,
	marker_name		varchar(255)	null,
	marker_id		varchar(30)	null,
	PRIMARY KEY(index_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'referenceKey' : 'create index %s on %s (reference_key)',
	'markerKey' : 'create index %s on %s (marker_key)',
	'jnum' : 'create index %s on %s (jnum_id)',
	'markerID' : 'create index %s on %s (marker_id)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
