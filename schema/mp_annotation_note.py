#!/usr/local/bin/python

import Table

# contains data definition information for the mp_annotation_note table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mp_annotation_note'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	mp_note_key		int		not null,
	mp_reference_key		int		not null,
	note		text		not null,
	PRIMARY KEY(mp_note_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'mp_reference_key' : 'create index %s on %s (mp_reference_key)',
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'represents the data needed to render mp note information on the allele and genotype detail pages ',
	Table.COLUMN : {
		'mp_note_key' : 'unique key identifying this annotation (does not correspond to _Annot_key in mgd)',
		'mp_reference_key' : 'foreign key to mp_reference',
		'note' : 'the annotation note',
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
