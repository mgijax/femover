#!/usr/local/bin/python

import Table

# contains data definition information for the sequence_location table

###--- Globals ---###

# name of this database table
tableName = 'sequence_location'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		NOT NULL,
	sequence_key		int		NOT NULL,
	sequence_num		int		NOT NULL,
	chromosome		varchar(8)	NULL,
	start_coordinate	float		NULL,
	end_coordinate		float		NULL,
	build_identifier	varchar(30)	NULL,
	location_type		varchar(20)	NULL,
	map_units		varchar(50)	NULL,
	provider		varchar(255)	NULL,
	version			varchar(30)	NULL,
	strand			varchar(1)	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'sequence_key' : 'create index %s on %s (sequence_key, sequence_num)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
