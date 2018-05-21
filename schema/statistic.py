#!/usr/local/bin/python

import Table

# contains data definition information for the statistic table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'statistic'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	statistic_key		int		not null,
	name			text		not null,
	tooltip			text	null,
	value			int			not null,
	group_name			text		not null,
	sequencenum			int		not null,
	group_sequencenum		int not	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'statistic_key' : 'create index %s on %s (statistic_key)',
	'name' : 'create index %s on %s (name)',
	'tooltip' : 'create index %s on %s (tooltip)',
	'group_name' : 'create index %s on %s (group_name)'
	}

keys = {
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains all the most recent data measurements',
	Table.COLUMN : {
		'unique_key' : 'unique key for this row',
		'statistic_key' : 'unique key for this statistic',
		'name' : 'long name of statistic',
		'tooltip' : 'mouse-over tooltip for the statistic',
		'value' : 'integer value measured for this statistic',
		'group_name' : 'what group is this statistic part of',
		'sequencenum' : 'Ordering of this statistic',
		'group_sequencenum' : 'Ordering of the group',
		},
	Table.INDEX : {
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
