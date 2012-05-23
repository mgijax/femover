#!/usr/local/bin/python

import Table

# contains data definition information for the expression_index_age_map table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_index_age_map'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	age_string		varchar(255)	not null,
	min_theiler_stage	int		not null,
	max_theiler_stage	int		not null,
	sequence_num		int		not null,
	PRIMARY KEY(age_string))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for expression literature flower, mapping from an age string (in DPC) to its equivalent Theiler stage range.  (convenient conversion for age_string values in expression_index_stages)',
	Table.COLUMN : {
		'age_string' : 'unique identifier for the record; the age string that needs to be converted',
		'min_theiler_stage' : 'minimum Theiler stage relating to this age_string',
		'max_theiler_stage' : 'maximum Theiler stage relating to this age_string',
		'sequence_num' : 'used for ordering age strings',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
