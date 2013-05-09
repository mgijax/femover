#!/usr/local/bin/python

import Table

# contains data definition information for the recombinase_system_structure table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'recombinase_system_structure'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	system_structure_key	int		not null,
	allele_system_key	int		not null,
	structure		text		null,
	structure_seq		int 		null,
        age_e1                  int             null,
        age_e2                  int             null,
        age_e3                  int             null,
        age_p1                  int             null,
        age_p2                  int             null,
        age_p3                  int             null,
        has_image               int             not null,
	PRIMARY KEY(system_structure_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_system_key' : 'create index %s on %s (allele_system_key)',
	}

keys = { 'allele_system_key' : ('recombinase_allele_system', 'allele_system_key'), }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'child of recombinase_allele_system table that contains distinct structures for the datatable in recombinase ribbon',
	Table.COLUMN : {
		'system_structure_key' : 'unique key for the system_structure pair',
		'allele_system_key' : 'unique key for the allele/system pair',
		'structure' : 'name of the anatomical structure',
                'age_e1' : 'embryonic age 0-8.9',
                'age_e2' : 'embryonic age 8.91-13.9',
                'age_e3' : 'embryonic age 14.0-21.0',
                'age_p1' : 'postnatal age 21.01-42.01',
                'age_p2' : 'postnatal age 42.02-63.01',
                'age_p3' : 'postnatal age 63.02-1846',
                'has_image' : '1 if this allele has at least one displayable image, 0 if not',
		},
	Table.INDEX : {
		'allele_system_key' : 'foreign key to recombinase_allele_system',
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
