#!/usr/local/bin/python

import Table

# contains data definition information for the template table
# (search for template to find areas that need changes)

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'uni_keystone'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uni_key			int	not null,
	is_classical	int	not null,
	assay_type_key	int	not null,
	assay_key		int	not null,
	old_result_key	int	not null,
	result_key		int	not null,
	ageMin			float	null,
	ageMax			float	null,
	_Stage_key		int	not null,
	_Term_key		int	not null,
	by_structure	int	not null,
	by_marker		int	not null,
	by_assay_type	int	not null,
	is_detected		int	not null,
	by_reference	int	not null,
	PRIMARY KEY(uni_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'is_classical' : 'create index %s on %s (is_classical)',
	'assay_key' : 'create index %s on %s (assay_key)',
	'old_result_key' : 'create index %s on %s (old_result_key)',
	'result_key' : 'create index %s on %s (result_key)',
	'_Stage_key' : 'create index %s on %s (_Stage_key)',
	'_Term_key' : 'create index %s on %s (_Term_key)',
	}

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : '',
	Table.COLUMN : {
		},
	Table.INDEX : {
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
