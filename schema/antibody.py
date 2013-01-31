#!/usr/local/bin/python

import Table

###--- Globals ---###

# name of this database table
tableName = 'antibody'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	antibody_key    	int	NOT NULL,
	name          	varchar(40) NULL,
	antibodyTypes	varchar(255) NULL,
	antibodyClass	varchar(255) NULL,
	organism	varchar(255) NULL,
	note		varchar(255) NULL,
	primary_id     	varchar(30) NULL,
	logical_db	varchar(80) NULL,
	clone_id	varchar(30) NULL,
	PRIMARY KEY(probe_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'clone_id' : 'create index %s on %s (clone_id)',
	'id' : 'create index %s on %s (primary_id)',
	}

keys = { 'probe_key' : ('probe', 'probe_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the probe flower, containing basic data for probes',
	Table.COLUMN : {
		'probe_key' : 'unique key for this probe; same as _Probe_key in mgd',
		'name' : 'name of the probe',
		'segment_type' : 'type of segment (genomic, oligo, primer, etc.)',
		'primary_id' : 'primary accession ID for the probe',
		'logical_db' : 'logical database (entity) assigning the ID',
		'clone_id' : 'clone ID associated with the probe',
		},
	Table.INDEX : {
		'clone_id' : 'lookup by clone ID',
		'id' : 'lookup by probe ID',
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
