#!/usr/local/bin/python

import Table

###--- Globals ---###

# name of this database table
tableName = 'marker'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	marker_key    	int	NOT NULL,
	symbol        	varchar(50) NULL,
	name          	varchar(255) NULL,
	marker_type	varchar(80) NULL,
	marker_subtype	varchar(50) NULL,
	organism      	varchar(50) NULL,
	primary_id     	varchar(30) NULL,
	logical_db	varchar(80) NULL,
	status       	varchar(255) NULL,
	has_go_graph	int	NOT NULL,
	has_go_orthology_graph	int	NOT NULL,
	is_in_reference_genome	int	NOT NULL,
	PRIMARY KEY(marker_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'symbol' : 'create index %s on %s (symbol)',
	'primary_id' : 'create index %s on %s (primary_id)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
