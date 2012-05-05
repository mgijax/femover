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
	marker_count		int	not null,
	expression_marker_count	int	not null,
	gxdlit_marker_count	int	not null,
	PRIMARY KEY(term_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'term_key' : ('term', 'term_key'), }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'stores several special counts for vocabulary terms, one row per vocabulary term',
	Table.COLUMN : {
		'term_key' : 'foreign key to term table, identifying the term with which we are concerned',
		'path_count' : 'number of paths down a DAG that lead to this term',
		'descendent_count' : 'number of descendents of this term',
		'child_count' : 'number of immediate children of this term',
		'marker_count' : 'number of markers associated with this term',
		'expression_marker_count' : 'number of markers associated with this term which also have fully-coded expression data',
		'gxdlit_marker_count' : 'number of markers associated with this term which are included in the GXD Literature Index',
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
