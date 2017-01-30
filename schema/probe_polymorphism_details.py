#!/usr/local/bin/python

import Table

# contains data definition information for the probe_polymorphism_details table

###--- Globals ---###

# name of this database table
tableName = 'probe_polymorphism_details'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key				int		NOT NULL,
	probe_polymorphism_key	int		NOT NULL,
	allele					text	NULL,
	fragments				text	NULL,
	strains					text	NULL,
	sequence_num			int		NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
	'probe_polymorphism_key' : ('probe_polymorphism', 'probe_polymorphism_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('probe_polymorphism_key', 'create index %s on %s (probe_polymorphism_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'defines details of probe polymorphism data (alleles, fragments, strains)',
	Table.COLUMN : {
		'unique_key' : 'genereted primary key of table (no other significance)',
		'probe_polymorphism_key' : 'identifies this polymorphism; same as prb_rflv._RFLV_key in prod',
		'allele' : 'identifier of the allele in the polymorphism; order by this for display purposes',
		'fragments' : 'sizes of fragments',
		'strains' : 'comma-delimited list of strains having this allele',
		'sequence_num' : 'pre-computed ordering based on probe_polymorphism_key, allele, strains',
		},
	Table.INDEX : {
		'probe_polymorphism_key' : 'clusters data so that all details for a polymorphism will be near each other on disk, aiding quick access',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
