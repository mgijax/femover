#!/usr/local/bin/python

import Table

# contains data definition information for the marker_sequence_num table

###--- Globals ---###

# name of this database table
tableName = 'marker_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	marker_key	int		not null,
	by_symbol	int		not null,
	by_marker_type	int		not null,
	by_organism	int		not null,
	by_primary_id	int		not null,
	by_location	int		not null,
	PRIMARY KEY(marker_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'marker_key' : ('marker', 'marker_key'), }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the marker flower, containing pre-computed sorts for each marker',
	Table.COLUMN : {
		'marker_key' : 'identifies the marker',
		'by_symbol' : 'sort by marker symbol, smart-alpha (handles embedded numbers intelligently)',
		'by_marker_type' : 'sort by marker type',
		'by_organism' : 'sort by organism containing the marker',
		'by_primary_id' : 'sort by primary ID',
		'by_location' : 'sort by location (preference to coordinates, then cM position, then cytogenetic band)',
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
