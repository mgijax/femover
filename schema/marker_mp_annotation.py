#!/usr/local/bin/python

import Table

# contains data definition information for the marker_mp_annotation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_mp_annotation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	mp_annotation_key	int		not null,
	mp_genotype_key		int		not null,
	qualifier		text		null,
	term_key		int		not null,
	term_id			text		not null,
	term			text		null,
	sequence_num		int		not null,
	PRIMARY KEY(mp_annotation_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'term_key' : 'create index %s on %s (term_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'mp_genotype_key' : ('marker_mp_genotype', 'mp_genotype_key'),
	'term_key' : ('term', 'term_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('mp_genotype_key', 'create index %s on %s (mp_genotype_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains data about one or more terms annotated to a marker/genotype pair (a record in the marker_mp_genotype table)',
	Table.COLUMN : {
		'mp_annotation_key' : 'generated primary key for this table',
		'mp_genotype_key' : 'foreign key to marker_mp_genotype, indicating the marker/genotype pair for this annotation',
		'qualifier' : 'qualifier term for the annotation',
		'term_key' : 'foreign key to the term table, identifies the annotated term',
		'term_id' : 'primary MP ID for the annotated term, cached here for convenience',
		'term' : 'text of the annotated term, cached here for convenience',
		'sequence_num' : 'sequence number for ordering terms annotated to a marker/genotype pair',
		},
	Table.INDEX : {
		'term_key' : 'quick access to all annotations for a given term',
		'mp_genotype_key' : 'clustered index, ensuring that all annotations for a given marker/genotype pair are stored together on disk',
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
