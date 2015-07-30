#!/usr/local/bin/python

import Table

# contains data definition information for the expression_index table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_index'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	index_key		int		not null,
	reference_key		int		not null,
	jnum_id			text	null,
	marker_key		int		not null,
	marker_symbol		text	null,
	marker_name		text	null,
	marker_id		text	null,
	comments		text	null,
	PRIMARY KEY(index_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'referenceKey' : 'create index %s on %s (reference_key)',
	'jnum' : 'create index %s on %s (jnum_id)',
	'markerID' : 'create index %s on %s (marker_id)',
	}

keys = {
	'marker_key' : ('marker', 'marker_key'),
	'reference_key' : ('reference', 'reference_key'),
	} 

# index used to cluster data in the table
clusteredIndex = ('markerKey', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the expression literature flower, where each record is a marker/reference pair (showing that expression was studied for that marker in that reference)',
	Table.COLUMN : {
		'index_key' : 'unique identifier for this marker/reference pair, same as _Index_key from mgd',
		'reference_key' : 'identifies the reference',
		'jnum_id' : 'J: number ID for the reference, cached for convenience',
		'marker_key' : 'identifies the marker',
		'marker_symbol' : 'symbol for the marker, cached for convenience',
		'marker_name' : 'name for the marker, cached for convenience',
		'marker_id' : 'primary ID for the marker, cached for convenience',
		'comments' : 'comments for this marker/reference pair',
		},
	Table.INDEX : {
		'referenceKey' : 'quick access to literature index records for a particular reference',
		'markerKey' : 'clusters data so that index records for a marker are nearby on disk, to aid quick lookups by marker',
		'jnum' : 'quick access by J: number',
		'markerID' : 'quick access by marker ID',
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
