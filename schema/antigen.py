#!/usr/local/bin/python

import Table

###--- Globals ---###

# name of this database table
tableName = 'antigen'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	antigen_key    	int	NOT NULL,
	name          	varchar(255) NULL,
	regioncovered	varchar(255) NULL,
	note		varchar(255) NULL,
	primary_id     	varchar(30) NULL,
	logical_db	varchar(80) NULL,
	PRIMARY KEY(antigen_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'id' : 'create index %s on %s (primary_id)',
	}

keys = { 'antigen_key' : ('antigen', 'antigen_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the antigen flower, containing basic data for antigens',
	Table.COLUMN : {
		'antigen_key' : 'unique key for this antigen; same as _Antigen_key in mgd',
		'name' : 'name of the antigen',
	        'regioncovered' : 'region covered by the antigen',
	        'note' : 'additional notes',
		'primary_id' : 'primary accession ID for the antigen',
		'logical_db' : 'logical database (entity) assigning the ID',
		},
	Table.INDEX : {
		'id' : 'lookup by antigen ID',
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
