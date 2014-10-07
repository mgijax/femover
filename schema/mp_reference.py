#!/usr/local/bin/python

import Table

# contains MP reference information for the genotype pheno summary  page

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mp_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	mp_reference_key		int	not null,
	mp_term_key			int	not null,
	mp_annotation_key		int	not null,
	jnum_id				text	null,
        phenotyping_center_key		int	null,
        interpretation_center_key	int	null,
	PRIMARY KEY(mp_reference_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'mp_term_key' : 'create index %s on %s (mp_term_key)',
	'mp_annotation_key' : 'create index %s on %s (mp_annotation_key)',
	'pc_key' : 'create index %s on %s (phenotyping_center_key)',
	'ic_key' : 'create index %s on %s (interpretation_center_key)',
}

keys = {
	'phenotyping_center_key' : ('phenotable_center', 'center_key'),
	'interpretation_center_key' : ('phenotable_center', 'center_key'),
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'represents the data needed to render phenotype information on the genotype detail pages ',
	Table.COLUMN : {
		'mp_reference_key' : 'unique key identifying this row',
		'mp_term_key' : 'key for the term',
		'mp_annotation_key' : 'key for the annotation',
		'jnum_id' : 'display name for an MP header term',
		'phenotyping_center_key' : 'identifies the provider serving as phenotyping center',
		'interpretation_center_key' : 'identifies the provider serving as data interpretation center',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
