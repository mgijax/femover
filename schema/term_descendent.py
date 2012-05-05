#!/usr/local/bin/python

import Table

# contains data definition information for the term_descendent table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_descendent'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	term_key		int		not null,
	descendent_term_key	int		not null,
	descendent_term		varchar(255)	null,
	descendent_primary_id	varchar(30)	null,
	sequence_num		int		null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'term_key' : 'create index %s on %s (term_key)',
	}

keys = {
	'term_key' : ('term', 'term_key'),
	'descendent_term_key' : ('term', 'term_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('term_key_seqnum',
	'create index %s on %s (term_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : None,
	Table.COLUMN : {
		'unique_key' : 'unique key for this term / descendent pair',
		'term_key' : 'foreign key to term table, identifies the term of interest',
		'descendent_term_key' : 'foreign key to term table, identifies a descendent term',
		'descendent_term' : 'name of the descendent term, cached for convenience',
		'descendent_primary_id' : 'primary accession ID of the descendent term, cached for convenience',
		'sequence_num' : 'sequence number, for ordering the descendents of a given term',
		},
	Table.INDEX : {
		'term_key' : 'quick access to descendents of a term',
		'term_key_seqnum' : 'cluster data by term key and sequence number so they will be ready to return',
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
