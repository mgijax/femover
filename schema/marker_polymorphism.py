#!/usr/local/bin/python

import Table

# contains data definition information for the marker_polymorphism table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_polymorphism'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	polymorphism_key	int		not null,
	marker_key			int		not null,
	polymorphism_type	text	not null,
	jnum_id				text	not null,
	probe_name			text	not null,
	probe_id			text	null,
	endonuclease		text	null,
	sequence_num		int		not null,
	PRIMARY KEY(polymorphism_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = { 'marker_key' : ('marker', 'marker_key'), }

# index used to cluster data in the table
clusteredIndex = ('mrk_poly', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'Is a petal table for the marker flower. Each row contains the basic data for a single polymorphism (type, reference, probe, endonuclease).',
	Table.COLUMN : {
		'polymorphism_key' : 'primary key; same as _RFLV_key in PRB_RFLV table in production databse',
		'marker_key' : 'foreign key to marker table, identifying the marker',
		'polymorphism_type' : 'type of polymorphism -- RFLV or PCR',
		'jnum_id' : 'J: number for the reference, cached here for convenience',
		'probe_name' : 'name of the probe used, cached here for convenience',
		'probe_id' : 'primary ID of the probe, cached here for convenience',
		'endonuclease' : 'name of endonuclease (an enzyme that cleaves its nucleic acid substrate at internal sites in the nucleotide sequence)',
		'sequence_num' : 'pre-computed sequence number for ordering polymorphisms of a marker',
		},
	Table.INDEX : {
		'mrk_poly' : 'clustered index; brings together polymorphism rows for each marker, providing fast retrieval',
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
