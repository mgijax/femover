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
	assay_type		text	not null,
	primary_id		text	null,
	probe_key		int		null,
	probe_name		text	null,
	antibody_key		int		null,
	antibody		text	null,
	detection_system	text	null,
	is_direct_detection	int		not null,
	probe_preparation	text	null,
	visualized_with		text	null,
	reporter_gene		text	null,
	note			text	null,
	has_image		int		not null,
	gel_imagepane_key		int		null,
	reference_key		int		not null,
	marker_key		int		null,
	marker_id		text	null,
	marker_symbol		text	null,
	marker_name		text	null,
        modification_date       text     null,
	PRIMARY KEY(assay_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'primary_id' : 'create index %s on %s (primary_id)',
	'marker_id' : 'create index %s on %s (marker_id)',
	}

keys = {
	'gel_imagepane_key' : ('expression_imagepane', 'imagepane_key'),
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
		'antibody_key' : 'identifies the antibody used',
		'antibody' : 'name of the antibody',
		'detection_system' : 'name of the detection system',
		'is_direct_detection' : '1 if this used direct detecction, 0 if not',
		'probe_preparation' : 'describes the probe preparation method',
		'visualized_with' : 'describes the visualization method',
		'reporter_gene' : 'symbol of the reporter gene',
		'note' : 'assay note',
		'has_image' : '1 if this assay has at least one displayable image, 0 if not',
		'gel_imagepane_key' : 'foreign key to expression_imagepane table. Null for in situ assays',
		'reference_key' : 'identifies the reference',
		'marker_key' : 'identifies the marker studied',
		'marker_id' : 'primary ID for the marker, cached here for convenience',
		'marker_symbol' : 'symbol for the marker, cached here for convenience',
		'marker_name' : 'name for the marker, cached here for convenience',
		'modification_date' : 'last time the assay record was modified, cached here for convenience',
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
