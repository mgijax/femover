#!/usr/local/bin/python

import Table

# contains data definition information for the phenotable_center table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'phenotable_center'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	center_key	int		not null,
	name		text	not null,
	abbreviation	text		null,
	sequence_num	int		not null,
	PRIMARY KEY(center_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'name' : 'create index %s on %s (name)',
	}

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'defines the phenotyping and interpretation centers for the phenotable_provider table',
	Table.COLUMN : {
		'center_key' : 'unique key for this table',
		'name' : 'center name',
		'abbreviation' : 'shorter code for this center',
		'sequence_num' : 'orders the various centers in this table',
		},
	Table.INDEX : {
		'name' : 'allows quick access by center name',
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
