#!/usr/local/bin/python

import Table

# contains data definition information for the term_child table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_child'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	term_key		int		not null,
	child_term_key		int		not null,
	child_term		varchar(255)	null,
	child_primary_id	varchar(30)	null,
	sequence_num		int		null,
	is_leaf			int		not null,
	edge_label		varchar(255)	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'term_key' : 'create index %s on %s (term_key)',
	}

keys = {
	'term_key' : ('term', 'term_key'),
	'child_term_key' : ('term', 'term_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
