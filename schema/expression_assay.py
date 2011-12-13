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
	'marker_key' : 'create index %s on %s (marker_key)',
	}

keys = {
	'probe_key' : ('probe', 'probe_key'),
	'marker_key' : ('marker', 'marker_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
