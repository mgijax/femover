#!/usr/local/bin/python

import Table

# contains data definition information for the strain_grid_heading table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_grid_heading'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	heading_key		int	not null,
	heading			text	not null,
	heading_abbreviation	text	null,
	grid_name		text	not null,
	grid_name_abbreviation	text	null,
	sequence_num		int	not null,
	PRIMARY KEY(heading_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'grid_name' : 'create index %s on %s (grid_name)',
	'grid_name_abbreviation' : 'create index %s on %s (grid_name_abbreviation)',
	}

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = ('sequence_num', 'create index %s on %s (sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'strain_grid_heading',
	Table.COLUMN : {
		'heading_key' : 'unique key for this table',
		'heading' : 'name of the heading',
		'heading_abbreviation' : '(optional) shorter version of the name',
		'grid_name' : 'name of the type of slimgrid',
		'grid_name_abbreviation' : '(optional) shorter version of the name of the grid',
		'sequence_num' : 'used to order the headings within a slimgrid',
		},
	Table.INDEX : {
		'grid_name' : 'get all headings for a slimgrid',
		'grid_name_abbreviation' : 'get all headings for a slimgrid abbreviation',
		'sequence_num' : 'used to cluster the heaidng records for fast ordering of queries',
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
