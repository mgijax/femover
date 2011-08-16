#!/usr/local/bin/python

import Table

# contains data definition info for the recombinase_assay_result table

###--- Globals ---###

# name of this database table
tableName = 'recombinase_assay_result'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	result_key		int		not null,
	allele_system_key	int		not null,
	structure		varchar(80)	null,
	age			varchar(50)	null,
	level			varchar(20)	null,
	pattern			varchar(30)	null,
	jnum_id			varchar(30)	null,
	assay_type		varchar(30)	null,
	reporter_gene		varchar(40)	null,
	detection_method	varchar(10)	null,
	sex			varchar(30)	null,
	allelic_composition	varchar(512)	null,
	background_strain	varchar(255)	null,
	assay_note		varchar(768)	null,
	result_note		varchar(255)	null,
	specimen_note		varchar(255)	null,
	probe_id		varchar(30)	null,
	probe_name		varchar(40)	null,
	antibody_id		varchar(30)	null,
	antibody_name		varchar(255)	null,
	PRIMARY KEY(result_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = { 'allele_system_key' : 'create index %s on %s (allele_system_key)',
	}

keys = { 'allele_system_key' : ('recombinase_allele_system', 'allele_system_key'), }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
