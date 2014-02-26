#!/usr/local/bin/python

import Table

# contains data definition information for the marker_related_allele table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_related_allele'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	mra_key			int	not null,
	marker_key		int	not null,
	related_allele_key	int	not null,
	related_allele_symbol	text	not null,
	related_allele_id	text	not null,
	relationship_category	text	not null,
	relationship_term	text	not null,
	qualifier		text	not null,
	evidence_code		text	not null,
	reference_key		int	not null,
	jnum_id			text	not null,
	sequence_num		int	not null,
	PRIMARY KEY(mra_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
	'marker_key' : ('marker', 'marker_key'),
	'related_allele_key' : ('allele', 'allele_key'),
	'reference_key' : ('reference', 'reference_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('markerSeqNum',
	'create index %s on %s (marker_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'A record in this table represents a relationship between a marker and an allele (separate from the traditional "This is an allele of this marker" relationship), described by the relationship term and its qualifier',
	Table.COLUMN : {
		'mra_key' : 'primary key, uniquely identifying a record in this table',
		'marker_key' : 'foreign key to marker table, identifying the base marker for this relationship',
		'related_allele_key' : 'foreign key to allele table, identifying the related allele',
		'related_allele_symbol' : 'symbol of the related allele, cached for convenience',
		'related_allele_id' : 'primary accession ID of the related allele, cached for convenience',
		'relationship_category' : 'text name for the category of relationship',
		'relationship_term' : 'term describing the relationship (drawn from terms in the category)',
		'qualifier' : 'modifier on the relationship',
		'evidence_code' : 'type of evidence used to support the relationship',
		'reference_key' : 'foreign key to reference table, identifying the citation supporting this relationship',
		'jnum_id' : 'accession ID (J: number) for the reference, cached for convenience',
		'sequence_num' : 'integer, used to order related alleles for a given base marker',
		},
	Table.INDEX : {
		'markerSeqNum' : 'clustered index ensures related alleles for a given marker are stored in order together on disk',
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
