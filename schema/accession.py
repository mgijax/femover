#!/usr/local/bin/python

import Table

# contains data definition information for the accession table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'accession'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	object_key		int		not null,
	search_id		varchar(30)	not null,
	display_id		varchar(30)	not null,
	sequence_num		smallint	not null,
	description		varchar(315)	null,
	logical_db_key		smallint	not null,
	display_type_key	smallint	not null,
	object_type_key		smallint	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'lower_acc_id' : 'create index %s on %s (lower(search_id))',
	'acc_id' : 'create index %s on %s (search_id)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
