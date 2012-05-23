#!/usr/local/bin/python

import Table

# contains data definition information for the sequence_gene_model table

###--- Globals ---###

# name of this database table
tableName = 'sequence_gene_model'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	sequence_key		int		NOT NULL,
	marker_type		varchar(80)	NULL,
	biotype			varchar(255)	NULL,
	exon_count		int		NULL,
	transcript_count	int		NULL,
	PRIMARY KEY(sequence_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'sequence_key' : ('sequence', 'sequence_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the sequence flower, containing extra data for gene model sequences',
	Table.COLUMN : {
		'sequence_key' : 'identifies the sequence',
		'marker_type' : 'identifies the type of marker, from the standard list of MGI marker types',
		'biotype' : 'what the provider identified the marker type to be, from their terminology',
		'exon_count' : 'number of exons',
		'transcript_count' : 'number of transcripts',
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
