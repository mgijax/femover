#!/usr/local/bin/python

import Table

# contains data definition information for the term_ancestor table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_ancestor'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	term_key		int		not null,
	ancestor_term_key	int		not null,
	ancestor_term		text	null,
	ancestor_primary_id	text	null,
	path_number		int		not null,
	depth			int		not null,
	edge_label		text	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'term_key' : 'create index %s on %s (term_key)',
	}

keys = {
	'term_key' : ('term', 'term_key'),
	'ancestor_term_key' : ('term', 'term_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('term_key_depth', 'create index %s on %s (term_key, depth)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains all ancestor terms for each term in the term table',
	Table.COLUMN : {
		'unique_key' : 'unique key for this term/ancestor pair',
		'term_key' : 'foreign key to term table',
		'ancestor_term_key' : 'foreign key to term table, for the ancestor term',
		'ancestor_term' : 'name of the ancestor term, cached here for convenience',
		'ancestor_primary_id' : 'accession ID of the ancestor term, cached here for convenience',
		'path_number' : 'for DAGs there can be multiple paths from a roo to a term; this identifies which path this ancestor is part of',
		'depth' : 'How far down the DAG is this ancestor node?  Root nodes are numbered 1.',
		'edge_label' : 'type of edge from this ancestor toward the descendent term',
		},
	Table.INDEX : {
		'term_key_depth' : 'cluster by term key and depth, so all ancestors of a term are close together and in order',
		'term_key' : 'quick lookup by term key',
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
