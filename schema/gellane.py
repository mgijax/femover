#!/usr/local/bin/python

import Table

# contains data definition information for the gellane table 

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'gellane'

# PgSQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	gellane_key		int		not null,
	assay_key		int		not null,
	genotype_key		int		not null,
	sex			text	null,
	age			text	null,
	age_note		text	null,
	lane_note		text	null,
	sample_amount		text	null,
	lane_label		text	null,
	control_text		text	null,
	is_control		smallint	not null,
	rna_type		text	null,
	lane_seq		int		not null,
	PRIMARY KEY(gellane_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'assay_key' : 'create index %s on %s (assay_key)',
	'genotype_key' : 'create index %s on %s (genotype_key)',
	}

keys = {
	'assay_key' : ('expression_assay', 'assay_key'),
	'genotype_key' : ('genotype', 'genotype_key'),
	}

clusteredIndex=None
# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'Contains information about specimens for expression_assay objects',
	Table.COLUMN : {
		'specimen_key' : 'unique identifier for this assay, same as _Specimen_key in mgd',
		'assay_key' : 'unique identifier for this assay, same as _Assay_key in mgd',
		'genotype_key' : 'unique identifier for this genotype, same as _Genotype_key in mgd',
		'sex' : 'sex for the specimen',
		'age' : 'long age string for the specimen',
		'age_note' : 'age note for the specimen',
		'lane_note' : 'gel lane note',
		'sample_amount' : 'gel lane sample amount',
		'lane_label' : 'gel lane label',
		'control_text' : 'if control, what type of control is it',
		'is_control' : '1 or 0 is this lane a control lane',
		'rna_type' : 'rna type for this gel lane',
		'lane_seq' : 'sort order for this gel lane',
		},
	Table.INDEX : {
		'assay_key' : 'foreign key to expression_assay',
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
