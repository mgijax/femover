#!/usr/local/bin/python

import Table

# contains data definition information for the marker_searchable_nomenclature
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_searchable_nomenclature'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	marker_key		int		not null,
	term			varchar(255)	not null,
	term_type		varchar(80)	not null,
	sequence_num		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'marker_key' : 'create index %s on %s (marker_key)',
	'term' : 'create index %s on %s (term)',
	'lower_term' : 'create index %s on %s (lower(term))',
	}

# column name -> (related table, column in related table)
keys = {
	'marker_key' : ('marker', 'marker_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
