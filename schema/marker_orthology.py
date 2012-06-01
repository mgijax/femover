#!/usr/local/bin/python

import Table

# contains data definition information for the marker_orthology table

###--- Globals ---###

# name of this database table
tableName = 'marker_orthology'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	NOT NULL,
	mouse_marker_key	int	NOT NULL,
	other_marker_key	int	NOT NULL,
	other_symbol		varchar(50)	NULL,
	other_organism		varchar(50)	NULL,
	sequence_num		int		NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'other_marker' : 'create index %s on %s (other_marker_key)',
	'other_organism' : 'create index %s on %s (other_organism)',
	}

keys = {
	'mouse_marker_key' : ('marker', 'marker_key'),
	'other_marker_key' : ('marker', 'marker_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('mouse_marker', 'create index %s on %s (mouse_marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal tablel for the marker flower, identifying the orthologous markers for each mouse marker',
	Table.COLUMN : {
		'unique_key' : 'unique key for this record, no other purpose',
		'mouse_marker_key' : 'identifies the mouse marker',
		'other_marker_key' : 'identifies the orthologous marker in another organism',
		'other_symbol' : 'symbol of the orthologous marker, cached for convenience',
		'other_organism' : 'organism containing the orthologous marker, cached for convenience',
		'sequence_num' : 'for ordering orthologous markers for a given mouse marker',
		},
	Table.INDEX : {
		'mouse_marker' : 'clusters data so all orthologs of a mouse marker are stored near each other on disk, to aid quick retrieval',
		'other_marker' : 'look up the mouse marker for which a non-mouse marker is an ortholog',
		'other_organism' : 'look up mouse markers which have orthologs in a given organism',
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
