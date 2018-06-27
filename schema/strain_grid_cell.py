#!/usr/local/bin/python

import Table

# contains data definition information for the strain_grid_cell table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_grid_cell'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	grid_cell_key	int	not null,
	strain_key	int	not null,
	heading_key	int	not null,
	color_level	int	not null,
	value		int	not null,
	sequence_num	int	not null,
	PRIMARY KEY(grid_cell_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'heading_key' : 'create index %s on %s (heading_key)',
	}

# column name -> (related table, column in related table)
keys = { 'strain_key' : ('strain', 'strain_key'),
	'heading_key' : ('strain_grid_heading', 'heading_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('strain_key', 'create index %s on %s (strain_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'strain_grid_cell',
	Table.COLUMN : {
		'grid_cell_key' : 'unique key for this table (no other purpose)',
		'strain_key' : 'identifies the strain (also used to cluster the rows in the table for efficient access)',
		'heading_key' : 'identifies the heading for this cell',
		'color_level' : 'identifies a color level for the cell',
		'value' : 'count of annotations for this cell',
		'sequence_num' : 'orders the cells for a strain; assumed to be in sync with sequence_num in strain_grid_heading',
		},
	Table.INDEX : {
		'heading_key' : 'quick access for a specific header term',
		'strain_key' : 'clustered index, to group records by strain for fast access',
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
