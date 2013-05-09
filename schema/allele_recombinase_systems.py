#!/usr/local/bin/python

import Table

# contains data definition information for the allele_recombinase_systems
# table

###--- Globals ---###

# name of this database table
tableName = 'allele_recombinase_systems'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_key			int		not null,
	detected_count			int		null,
	not_detected_count		int		null,
	PRIMARY KEY(allele_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the allele flower, containing summary data for recombinase activity of each allele',
	Table.COLUMN : {
		'allele_key' : 'identifies the allele',
		'detected_count' : 'count of systems where recombinase activity was detected',
		'not_detected_count' : 'count of systems where recombinase activity was studied and was not detected',
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
