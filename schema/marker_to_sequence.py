#!/usr/local/bin/python

import Table

# contains data definition information for the marker_to_sequence table

###--- Globals ---###

# name of this database table
tableName = 'marker_to_sequence'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		NOT NULL,
	marker_key	int		NOT NULL,
	sequence_key	int 		NOT NULL,
	reference_key	int		NOT NULL,
	qualifier	varchar(80)	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'sequence_key' : 'create index %s on %s (sequence_key, marker_key)',
	'reference_key' : 'create index %s on %s (reference_key)',
	}

keys = {
	'marker_key' : ('marker', 'marker_key'),
	'sequence_key' : ('sequence', 'sequence_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('marker_key',
	'create index %s on %s (marker_key, sequence_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between the marker and sequence flowers',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other purpose',
		'marker_key' : 'identifies the marker',
		'sequence_key' : 'identifies a sequence for this marker',
		'reference_key' : 'identifies the reference supporting this association',
		'qualifier' : 'qualifier describing this association (representative sequences for each marker are tagged as genomic, transcript, or polypeptide)',
		},
	Table.INDEX : {
		'marker_key' : 'clusters data so that all sequence associations for a marker are stored together on disk, aiding quick retrieval',
		'sequence_key' : 'look up marker(s) for a sequence',
		'reference_key' : 'look up all marker/sequence associations supported by a given reference',
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
