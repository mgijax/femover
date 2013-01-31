#!/usr/local/bin/python

import Table

###--- Globals ---###

# name of this database table
tableName = 'antibody'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	antibody_key    	int	NOT NULL,
	name          	varchar(255) NULL,
	antibodyClass	varchar(255) NULL,
	antibodyType	varchar(255) NULL,
	organism	varchar(255) NULL,
	note		varchar(255) NULL,
	primary_id     	varchar(30) NULL,
	logical_db	varchar(80) NULL,
	PRIMARY KEY(antibody_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'id' : 'create index %s on %s (primary_id)',
	}

keys = { 'antibody_key' : ('antibody', 'antibody_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the antibody flower, containing basic data for antibodys',
	Table.COLUMN : {
		'antibody_key' : 'unique key for this antibody; same as _antibody_key in mgd',
		'name' : 'name of the antibody',
	        'antibodyClass' : 'antibody class',
	        'antibodyType' : 'antibody type',
	        'organism' : 'organism',
	        'note' : 'additional note',
	        'primary_id' : 'primary accession ID for the antibody',
		'logical_db' : 'logical database (entity) assigning the ID',
		},
	Table.INDEX : {
		'id' : 'lookup by antibody ID',
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
