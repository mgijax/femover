#!/usr/local/bin/python

import Table

# contains data definition information for the strain_qtl table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_qtl'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	strain_key		int		not null,
	marker_key		int		null,
	marker_symbol	text	null,
	marker_name		text	null,
	marker_id		text	null,
	allele_key		int		null,
	allele_symbol	text	null,
	allele_id		text	null,
	sequence_num	int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_key' : 'create index %s on %s (allele_key)',
	'marker_key' : 'create index %s on %s (marker_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'strain_key' : ('strain', 'strain_key'),
	'marker_key' : ('marker', 'marker_key'),
	'allele_key' : ('allele', 'allele_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('strain_key', 'create index %s on %s (strain_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for strain flower, containing QTL alleles and markers for strains',
	Table.COLUMN : {
		'unique_key' : 'primary key for this table, no other purpose',
		'strain_key' : 'identifies the strain (FK to strain table)',
		'marker_key' : 'identifies the QTL marker (FK to marker table)',
		'marker_symbol' : 'marker symbol, cached for convenience',
		'marker_name' : 'marker name, cached for convenience',
		'marker_id' : 'primary ID of marker, cached for convenience',
		'allele_key' : 'identifies the QTL allele (FK to allele table)',
		'allele_symbol' : 'allele symbol, cached for convenience',
		'allele_id' : 'primary ID of allele, cached for convenience',
		'sequence_num' : 'generated sequence number for ordering records for each strain',
		},
	Table.INDEX : {
		'allele_key' : 'quickly find all strains for a QTL allele',
		'marker_key' : 'quickly find all strains for a QTL marker',
		'strain_key' : 'clustered index to group the rows together for each strain on disk',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
