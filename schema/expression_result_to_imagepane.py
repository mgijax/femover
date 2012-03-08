#!/usr/local/bin/python

import Table

# contains data definition information for the expression_result_to_imagepane
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_result_to_imagepane'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	not null,
	result_key	int	not null,
	imagepane_key	int	not null,
	sequence_num	int	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'result_key' : 'create index %s on %s (result_key)',
	'imagepane_key' : 'create index %s on %s (imagepane_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'result_key' : ('expression_result_summary', 'result_key'),
	'imagepane_key' : ('expression_imagepane', 'imagepane_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
