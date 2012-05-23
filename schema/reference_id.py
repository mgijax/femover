#!/usr/local/bin/python

import Table

# contains data definition information for the reference_id table

###--- Globals ---###

# name of this database table
tableName = 'reference_id'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int NOT NULL,
	reference_key 	int NOT NULL,
	logical_db    	varchar(80) NULL,
	acc_id	  	varchar(30) NULL,
	preferred	int NOT NULL,
	private		int NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'acc_id' : 'create index %s on %s (acc_id)',
	}

keys = { 'reference_key' : ('reference', 'reference_key') }

# index used to cluster data in the table
clusteredIndex = ('reference_key', 'create index %s on %s (reference_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the reference flower, contains accession IDs for each reference',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this record, no relationship to anything else',
		'reference_key' : 'identifies the reference with this ID',
		'logical_db' : 'logical database (the entity assigning this ID)',
		'acc_id' : 'accession ID',
		'preferred' : '1 if this is the preferred ID for this reference assigned by this logical_db, or 0 if another is preferred',
		'private' : '1 if the ID should be considered private, or 0 if it is free to display it',
		},
	Table.INDEX : {
		'reference_key' : 'used to cluster IDs together, so those for a given reference can be retrieved quickly',
		'acc_id' : 'provides lookup by accession ID',
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
