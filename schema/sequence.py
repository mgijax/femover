#!/usr/local/bin/python

import Table

# contains data definition information for the sequence table

###--- Globals ---###

# name of this database table
tableName = 'sequence'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	sequence_key	int		NOT NULL,
	sequence_type	varchar(30)	NOT NULL,
	quality		varchar(30)	NULL,
	status		varchar(30)	NOT NULL,
	provider	varchar(255)	NOT NULL,
	organism	varchar(50)	NOT NULL,
	length		int		NULL,
	description	varchar(255)	NULL,
	version		varchar(15)	NULL,
	division	varchar(3)	NULL,
	is_virtual	int		NOT NULL,
	has_clone_collection	int	NOT NULL,
	sequence_date	varchar(12)	NULL,
	record_date	varchar(12)	NULL,
	primary_id	varchar(30)	NULL,
	logical_db	varchar(80)	NULL,
	library		varchar(255)	NULL,
	PRIMARY KEY(sequence_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'primary_id' : 'create index %s on %s (primary_id)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
