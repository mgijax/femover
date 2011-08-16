#!/usr/local/bin/python

import Table

# contains data definition information for the term_counts table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_counts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	term_key		int	not null,
	path_count		int	not null,
	descendent_count	int	not null,
	child_count		int	not null,
	PRIMARY KEY(term_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'term_key' : ('term', 'term_key'), }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
