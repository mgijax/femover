#!/usr/local/bin/python

import Table

# contains data definition information for the expression_assay table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_assay'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	assay_key		int		not null,
	assay_type		varchar(80)	not null,
	primary_id		varchar(40)	null,
	probe_key		int		null,
	probe_name		varchar(40)	null,
	antibody		varchar(255)	null,
	detection_system	varchar(510)	null,
	is_direct_detection	int		not null,
	probe_preparation	varchar(1024)	null,
	visualized_with		varchar(300)	null,
	reporter_gene		varchar(50)	null,
	note			varchar(2048)	null,
	has_image		int		not null,
	reference_key		int		not null,
	marker_key		int		null,
	marker_id		varchar(40)	null,
	marker_symbol		varchar(50)	null,
	PRIMARY KEY(assay_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'primary_id' : 'create index %s on %s (primary_id)',
	'marker_id' : 'create index %s on %s (marker_id)',
	}

keys = {
	'probe_key' : ('probe', 'probe_key'),
	'marker_key' : ('marker', 'marker_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the expression flower, containing basic data about gene expression assays',
	Table.COLUMN : {
		'assay_key' : 'unique identifier for this assay, same as _Assay_key in mgd',
		'assay_type' : 'type of expression assay',
		'primary_id' : 'primary accession ID for this assay',
		'probe_key' : 'identifies the probe used',
		'probe_name' : 'name of the probe, cached here from the probe table for convenience',
		'antibody' : 'name of the antibody',
		'detection_system' : 'name of the detection system',
		'is_direct_detection' : '1 if this used direct detecction, 0 if not',
		'probe_preparation' : 'describes the probe preparation method',
		'visualized_with' : 'describes the visualization method',
		'reporter_gene' : 'symbol of the reporter gene',
		'note' : 'assay note',
		'has_image' : '1 if this assay has at least one displayable image, 0 if not',
		'reference_key' : 'identifies the reference',
		'marker_key' : 'identifies the marker studied',
		'marker_id' : 'primary ID for the marker, cached here for convenience',
		'marker_symbol' : 'symbol for the marker, cached here for convenience',
		},
	Table.INDEX : {
		'primary_id' : 'quick access by assay ID',
		'marker_id' : 'quick access by marker ID',
		'marker_key' : 'clusters data so that assays for a marker are grouped together on disk for speed of access',
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
