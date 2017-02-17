#!/usr/local/bin/python

import Table

# contains data definition information for the mapping_table_cell table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_table_cell'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	cell_key		int		not null,
	row_key			int		not null,
	marker_id		text	null,
	label			text	null,
	sequence_num	int		not null,
	PRIMARY KEY(cell_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
	'row_key' : ('mapping_table_row', 'row_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('row_key', 'create index %s on %s (row_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'Each record stores data for a single cell of a single row of a single mapping experiment table.',
	Table.COLUMN : {
		'cell_key' : 'generated primary key for this table; no other significance',
		'row_key' : 'foreign key to mapping_table_row, identifying the row containing this cell',
		'marker_id' : '(optional) if label field is a marker symbol, can include a marker ID here to use in making a link',
		'label' : 'text to show in the table cell',
		'sequence_num' : 'sequence number for ordering the cells of a row, ascending from left to right',
		},
	Table.INDEX : {
		'row_key' : 'clustered index to keep all cells of a mapping experiment table row together on disk for fast access',
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
