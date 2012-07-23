#!/usr/local/bin/python

import Table

# contains data definition information for the expression_result_summary table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.
#
# 07/23/2012    lec
#       - TR10269
#       - added 'mgd_structure_key'
#       - this field represents the true GXD_Structure.structure_key value

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
	age_abbreviation	varchar(30)	null,
	age_min			float		null,
	age_max			float		null,
	structure		varchar(80)	null,
	structure_printname	varchar(255)	null,
	structure_key		int		null,
	mgd_structure_key	int		null,
	detection_level		varchar(255)	null,
	is_expressed		varchar(20)	null,
	reference_key		int		null,
	jnum_id			varchar(40)	null,
	has_image		int		not null,
	genotype_key		int		null,
	is_wild_type		int		not null,
	pattern			varchar(80)	null,
	PRIMARY KEY(result_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.

clusteredIndex = ('clusteredIndex', 'create index %s on %s (marker_key, assay_key)')

indexes = {
	'assay_key' : 'create index %s on %s (assay_key)',
	'marker_key' : 'create index %s on %s (marker_key)',
	'structure_key' : 'create index %s on %s (structure_key)',
	'mgd_structure_key' : 'create index %s on %s (mgd_structure_key)',
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

comments = {
	Table.TABLE : 'central table of expression result flower; stores individual expression results and associated data',
	Table.COLUMN : {
	    'result_key' : 'unique key identifying an expression result; note that this does not correspond to GXD_Expression._Result_key in the mgd database',
	    'assay_key' : 'specifies the assay with which this result is associated',
	    'assay_type' : 'specifies the type of the expression assay',
	    'assay_id' : 'accession ID for the assay',
	    'marker_key' : 'foreign key to the marker table, specifying which marker was assayed',
	    'marker_symbol' : 'caches the symbol of the assayed marker',
	    'anatomical_system' : 'anatomical system containing the reported structure',
	    'theiler_stage' : 'developmental stage for the structure',
	    'age' : 'textual age, as recorded by MGI curators',
	    'age_abbreviation' : 'abbreviated age for web display',
	    'age_min' : 'computed minimum age (in DPC), based on textual age',
	    'age_max' : 'computed maximum age (in DPC), based on textual age',
	    'structure' : 'name of the anatomical structure in which expression was studied',
	    'structure_printname' : 'includes names of any parent nodes for structure, to establish context to uniquely identify the structure from its name',
	    'structure_key' : 'unique key for the given structure/term',
	    'mgd_structure_key' : 'unique mgd key for the given structure',
	    'detection_level' : 'strength of expression as reported in the literature',
	    'is_expressed' : 'summarization of detection_level -- was expression detected?',
	    'reference_key' : 'foreign key to reference table; identifies the source of the expression data',
	    'jnum_id' : 'accession ID for the reference',
	    'has_image' : 'does this expression result have an image available for display?',
	    'genotype_key' : 'foreign key to genotype table',
	    'is_wild_type' : 'is this a wild-type expression result?',
	    'pattern' : 'describes the pattern of expression observed',
		},
	Table.INDEX : {
	    'clusteredIndex' : 'used to group data for a marker and its assays together, for speed of access since this is a common use-case',
	    'assay_key' : 'allows quick access to all results for a particular assay',
	    'marker_key' : 'allows quick access to all results for a marker',
	    'structure_key' : 'allows quick access to all results for a given anatomical structure',
	    'reference_key' : 'allows quick access to all results for a given reference',
	    'genotype_key' : 'allows quick access for all results for a given genotype',
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
