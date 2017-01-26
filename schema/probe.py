#!/usr/local/bin/python

import Table

###--- Globals ---###

# name of this database table
tableName = 'probe'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	probe_key    	int	NOT NULL,
	name          	text NULL,
	segment_type	text NULL,
	primary_id     	text NULL,
	logical_db		text NULL,
	clone_id		text NULL,
	organism		text NULL,
	age				text NULL,
	sex				text NULL,
	cell_line		text NULL,
	vector			text NULL,
	insert_site		text NULL,
	insert_size		text NULL,
	product_size	text NULL,
	library			text NULL,
	tissue			text NULL,
	region_covered	text NULL,
	strain			text NULL,
	expression_result_count		int	NOT NULL,
	PRIMARY KEY(probe_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'clone_id' : 'create index %s on %s (clone_id)',
	'primary_id' : 'create index %s on %s (primary_id)',
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
		'organism' : 'source organism of the probe',
		'age' : 'age of source',
		'sex' : 'sex of source',
		'cell_line' : 'cell line of source',
		'vector' : 'segment vector type',
		'insert_site' : 'site of probe on the vector',
		'insert_size' : 'size of probe',
		'product_size' : 'size of probe product',
		'library' : 'name of source',
		'tissue' : 'tissue of source',
		'region_covered' : 'sequence region where probe hybridizes',
		'strain' : 'strain of the source',
		'expression_result_count' : 'number of expression results using this probe',
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
