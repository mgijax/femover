#!/usr/local/bin/python

import Table

# contains data definition information for the batch_marker_mp_annotations
# table

###--- Globals ---###

# name of this database table
tableName = 'batch_marker_mp_annotations'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		not null,
	marker_key	int		not null,
	mp_id		text	null,
	mp_term		text	null,
	sequence_num	int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'marker_key' : ('marker', 'marker_key') } 

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the marker flower, caching minimal data for mammalian phenotype (MP) annotations for the alleles of each marker',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this marker/term pair',
		'marker_key' : 'identifies the marker',
		'mp_id' : 'accession ID for the term (cached from the annotation table)',
		'mp_term' : 'text of the term (cached from the annotation table)',
		'sequence_num' : 'used for ordering MP terms for each marker',
		},
	Table.INDEX : {
		'marker_key' : 'clusters MP terms together for each marker, for speed in accessing them by marker',
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
