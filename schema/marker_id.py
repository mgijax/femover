#!/usr/local/bin/python

import Table

# contains data definition information for the markerID table

###--- Globals ---###

# name of this database table
tableName = 'marker_id'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	NOT NULL,
	marker_key	int	NOT NULL,
	logical_db	varchar(80) NULL,
	acc_id		varchar(30) NULL,
	preferred	int	NOT NULL,
	private		int	NOT NULL,
	is_for_other_db_section	int	NOT NULL,
	sequence_num	int	NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'acc_id' : 'create index %s on %s (acc_id)',
	}

keys = { 'marker_key' : ('marker', 'marker_key') }

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the marker flower, containing various accession IDs assigned to markers',
	Table.COLUMN : {
		'unique_key' : 'unique key for this marker/ID pair',
		'marker_key' : 'identifies the marker',
		'logical_db' : 'logical database assigning the ID',
		'acc_id' : 'accession ID',
		'preferred' : '1 if this is the preferred ID for this marker by this logical database, 0 if not',
		'private' : '1 if this ID should be considered private, 0 if it can be displayed',
		'is_for_other_db_section' : '1 if this ID should appear in the Other Database Links section of the marker detail page, 0 if not',
		'sequence_num' : 'orders IDs for a marker',
		},
	Table.INDEX : {
		'marker_key' : 'clusters data so all IDs for a marker are stored near each other on disk, to aid quick retrieval',
		'acc_id' : 'look up a marker by its ID',
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
