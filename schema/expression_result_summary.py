#!/usr/local/bin/python

import Table

# contains data definition information for the expression_result_summary table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_result_summary'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	result_key		int		not null,
	assay_key		int		not null,
	assay_type		varchar(80)	not null,
	assay_id		varchar(40)	null,
	marker_key		int		null,
	marker_symbol		varchar(50)	null,
	anatomical_system	varchar(255)	null,
	theiler_stage		varchar(5)	null,
	age			varchar(50)	null,
	structure		varchar(80)	null,
	structure_printname	varchar(255)	null,
	structure_key		int		null,
	detection_level		varchar(255)	null,
	is_expressed		varchar(20)	null,
	reference_key		int		null,
	jnum_id			varchar(40)	null,
	has_image		int		not null,
	genotype_key		int		null,
	PRIMARY KEY(result_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'assay_key' : 'create index %s on %s (assay_key)',
	'marker_key' : 'create index %s on %s (marker_key)',
	'structure_key' : 'create index %s on %s (structure_key)',
	'reference_key' : 'create index %s on %s (reference_key)',
	'genotype_key' : 'create index %s on %s (genotype_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'assay_key' : ('expression_assay', 'assay_key'),
	'marker_key' : ('marker', 'marker_key'),
	'structure_key' : ('term', 'term_key'),
	'reference_key' : ('reference', 'reference_key'),
	'genotype_key' : ('genotype', 'genotype_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
