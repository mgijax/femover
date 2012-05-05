#!/usr/local/bin/python

import Table

# contains data definition information for the term_sibling table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_sibling'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	term_key		int		not null,
	sibling_term_key	int		not null,
	sibling_term		varchar(255)	null,
	sibling_primary_id	varchar(30)	null,
	sequence_num		int		null,
	is_leaf			int		not null,
	edge_label		varchar(255)	null,
	path_number		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'term_key' : 'create index %s on %s (term_key)',
	}

keys = {
	'term_key' : ('term', 'term_key'),
	'sibling_term_key' : ('term', 'term_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('term_key_seqnum',
	'create index %s on %s (term_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'stores sibling terms for each term in the term table which is included in a DAG',
	Table.COLUMN : {
		'unique_key' : 'unique key for this term / sibling pair',
		'term_key' : 'foreign key to term table, identifying the term we are considering',
		'sibling_term_key' : 'foreign key to term table, identifying a sibling of the term',
		'sibling_term' : 'name of the sibling, cached for convenience',
		'sibling_primary_id' : 'ID of the sibling, cached for convenience',
		'sequence_num' : 'sequence number, for ordering siblings of a term',
		'is_leaf' : '1 if the sibling is a leaf node, 0 if not',
		'edge_label' : 'type of edge between the parent node and the sibling',
		'path_number' : 'a term can be arrived at through many paths; this number identifies the path we are considering',
		},
	Table.INDEX : {
		'term_key' : 'quick retrieval of siblings of a given term',
		'term_key_seqnum' : 'cluster data by term and sequence number, so all records for a term a together on disk',
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
