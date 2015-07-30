#!/usr/local/bin/python

import Table

# contains data definition information for the probeToSequence table

###--- Globals ---###

# name of this database table
tableName = 'probe_to_sequence'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		NOT NULL,
	probe_key	int		NOT NULL,
	sequence_key	int 		NOT NULL,
	reference_key	int		NOT NULL,
	qualifier	text	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'probe_key' : 'create index %s on %s (probe_key, sequence_key)',
	'sequence_key' : 'create index %s on %s (sequence_key, probe_key)',
	'reference_key' : 'create index %s on %s (reference_key)',
	}

keys = {
	'probe_key' : ('probe', 'probe_key'),
	'sequence_key' : ('sequence', 'sequence_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'join table between the probe and sequence flowers, matching probes with their sequences',
	Table.COLUMN : {
		'unique_key' : 'unique key for this record',
		'probe_key' : 'identifies the probe',
		'sequence_key' : 'identifies the sequence',
		'reference_key' : 'identifies the reference for this association',
		'qualifier' : 'qualifier describing this association',
		},
	Table.INDEX : {
		'probe_key' : 'look up sequences for a probe',
		'sequence_key' : 'look up probes for a sequence',
		'reference_key' : 'look up all probe/sequence pairs described in a reference',
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
