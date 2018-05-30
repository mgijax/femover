#!/usr/local/bin/python

import Table

# contains data definition information for the batch_marker_terms table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'batch_marker_terms'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	term			text	not null,
	term_type		text	not null,
	marker_key			int		null,
	strain_marker_key	int		null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'term' : 'create index %s on %s (term, term_type)',
	'lower_term' : 'create index %s on %s (lower(term), term_type)',
	'marker_key' : 'create index %s on %s (marker_key)',
	'strain_marker_key' : 'create index %s on %s (strain_marker_key)',
	}

keys = {
	'marker_key' : ('marker', 'marker_key'),
	'strain_marker_key' : ('strain_marker', 'strain_marker_key'),
	 } 

# index used to cluster data in the table
clusteredIndex = ('clustered_lower_term', 'create index %s on %s (lower(term))')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for marker flower, containing all terms by which we can search for markers using the batch query interface',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this record',
		'term' : 'term by which to search',
		'term_type' : 'identifies the type of term',
		'marker_key' : 'identifies the marker (if record is for a canonical marker)',
		'strain_marker_key' : 'identifies the strain marker (if record is for a strain-specific marker)',
		},
	Table.INDEX : {
		'clustered_lower_term' : 'clusters data by lowercased version of the term, as this is the most common search case',
		'term' : 'for searching by case sensitive term and term type',
		'lower_term' : 'for searching by lower(term) and term type',
		'marker_key' : 'to find all terms by which a given canonical marker can be found',
		'strain_marker_key' : 'to find all terms by which a given strain marker can be found',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
