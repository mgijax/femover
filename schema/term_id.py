#!/usr/local/bin/python

import Table

# contains data definition information for the term_id table

###--- Globals ---###

# name of this database table
tableName = 'term_id'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	NOT NULL,
	term_key	int	NOT NULL,
	logical_db	text NULL,
	acc_id		text NULL,
	preferred	int	NOT NULL,
	private		int	NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'acc_id' : 'create index %s on %s (acc_id)',
	}

keys = { 'term_key' : ('term', 'term_key'), }

# index used to cluster data in the table
clusteredIndex = ('term_key', 'create index %s on %s (term_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains accession IDs for vocabulary terms',
	Table.COLUMN : {
		'unique_key' : 'unique key for this term / ID / logical db combination',
		'term_key' : 'foreign key to term table, identifies the term to which this ID belongs',
		'logical_db' : 'name of the entity assigning the accession ID',
		'acc_id' : 'accession ID for the term',
		'preferred' : '1 if this is the preferred ID for this term from this logical db, 0 if not',
		'private' : '1 if this ID should be considered to be private, 0 if it can be displayed publicly',
		},
	Table.INDEX : {
		'term_key' : 'cluster data by term key, so IDs for the same term are grouped together on disk',
		'acc_id' : 'lookup by accession ID, case sensitive',
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
