#!/usr/local/bin/python

import Table

# contains data definition information for the expression_index_sequence_num
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_index_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	index_key	int	not null,
	by_reference	int	not null,
	by_marker	int	not null,
	PRIMARY KEY(index_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'index_key' : ('expression_index', 'index_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the expression literature flower, containing pre-computed sorts for ordering index entries',
	Table.COLUMN : {
		'index_key' : 'identifies the literature index record',
		'by_reference' : 'sort by reference (year, ID, reference key)',
		'by_marker' : 'sort by marker symbol',
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
