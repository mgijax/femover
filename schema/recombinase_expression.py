#!/usr/local/bin/python

import Table

# contains data definition information for the recombinase_expression table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'recombinase_expression'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	driver_key		int		not null,
	allele_key		int		not null,
	structure_key	int		not null,
	result_key		int		not null,
	is_detected		text	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'driver_key' : 'create index %s on %s (driver_key)',
	'allele_key' : 'create index %s on %s (allele_key)',
	'structure_key' : 'create index %s on %s (structure_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'driver_key' : ('marker', 'marker_key'),
	'allele_key' : ('allele', 'allele_key'),
	'structure_key' : ('term', 'term_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'table of expression result data for use in generating recombinaseMatrix Solr index',
	Table.COLUMN : {
		'unique_key' : 'generated primary key for this table',
		'driver_key' : '(marker) key for the driver of the recombinase allele',
		'allele_key' : 'identifies the recombinase allele',
		'structure_key' : 'EMAPS structure key for the result',
		'result_key' : 'key from production GXD_InSituResult table (not used otherwise in front-end db)',
		'is_detected' : 'was expression detected?  (Yes, No, Ambiguous)',
		},
	Table.INDEX : {
		'driver_key' : 'index by (marker) key of driver for recombinase allele',
		'allele_key' : 'index by the recombinase allele itself',
		'structure_key' : 'index by the EMAPA structure studied',
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
