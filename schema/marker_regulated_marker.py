#!/usr/local/bin/python

import Table

# contains data definition information for the marker_regulated_marker table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_regulated_marker'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	reg_key			int	not null,
	marker_key		int	not null,
	regulated_marker_key	int	not null,
	regulated_marker_symbol	text	not null,
	regulated_marker_id	text	not null,
	relationship_category	text	not null,
	relationship_term	text	not null,
	qualifier		text	not null,
	evidence_code		text	not null,
	reference_key		int	not null,
	jnum_id			text	not null,
	sequence_num		int	not null,
	in_teaser		int	not null,
	is_reversed		int	not null,
	PRIMARY KEY(reg_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'forTeaser' :
	    'create index %s on %s (marker_key, sequence_num, in_teaser)',
	}

# column name -> (related table, column in related table)
keys = {
	'marker_key' : ('marker', 'marker_key'),
	'regulated_marker_key' : ('marker', 'marker_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('markerSeqNum',
	'create index %s on %s (marker_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'A record in this table represents a relationship between two markers, described by the relationship term and its qualifier',
	Table.COLUMN : {
		'reg_key' : 'primary key, uniquely identifying a record in this table',
		'marker_key' : 'foreign key to marker table, identifying the base marker for this relationship',
		'regulated_marker_key' : 'foreign key to marker table, identifying the regulated marker',
		'regulated_marker_symbol' : 'symbol of the regulated marker, cached for convenience',
		'regulated_marker_id' : 'primary accession ID of the regulated marker, cached for convenience',
		'relationship_category' : 'text name for the category of relationship',
		'relationship_term' : 'term describing the relationship (drawn from terms in the category)',
		'qualifier' : 'modifier on the relationship',
		'evidence_code' : 'type of evidence used to support the relationship',
		'reference_key' : 'foreign key to reference table, identifying the citation supporting this relationship',
		'jnum_id' : 'accession ID (J: number) for the reference, cached for convenience',
		'sequence_num' : 'integer, used to order regulated markers for a given base marker',
		'in_teaser' : 'integer, indicates whether this marker should be included as a teaser (1) or not (0) on the marker detail page for marker_key',
		},
	Table.INDEX : {
		'markerSeqNum' : 'clustered index ensures regulated markers for a given marker are stored in order together on disk',
		'forTeaser' : 'provides quick access to markers which should be included as teasers on the marker detail page for other markers',
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
