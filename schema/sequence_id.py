#!/usr/local/bin/python

import Table

# contains data definition information for the sequence_id table

###--- Globals ---###

# name of this database table
tableName = 'sequence_id'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	NOT NULL,
	sequence_key	int NOT NULL,
	logical_db	varchar(80) NULL,
	acc_id		varchar(30) NULL,
	preferred	int	NOT NULL,
	private		int NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'acc_id' : 'create index %s on %s (acc_id)',
	}

keys = { 'sequence_key' : ('sequence', 'sequence_key') }

# index used to cluster data in the table
clusteredIndex = ('sequence_key', 'create index %s on %s (sequence_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the sequence flower, containing accession IDs for sequences',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record',
		'sequence_key' : 'identifies the sequence for this ID',
		'logical_db' : 'entity who assigned this ID',
		'acc_id' : 'accession ID',
		'preferred' : '1 if this is the preferred ID for this sequence from this logical_db, 0 if not',
		'private' : '1 if this ID should be considered to be private, 0 if it can be displayed',
		},
	Table.INDEX : {
		'sequence_key' : 'clusters IDs for each sequence ID, so we can quickly retrieve all the IDs for a sequence',
		'acc_id' : 'easily find sequences by their IDs',
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
