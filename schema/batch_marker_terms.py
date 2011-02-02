#!/usr/local/bin/python

import Table

# contains data definition information for the batch_marker_terms table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'batch_marker_terms'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	term			varchar(255)	not null,
	term_type		varchar(80)	not null,
	marker_key		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'term' : 'create index %s on %s (term, term_type)',
	'lower_term' : 'create index %s on %s (lower(term), term_type)',
	'marker_key' : 'create index %s on %s (marker_key)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
