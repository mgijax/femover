#!/usr/local/bin/python

import Table

# contains MP term information for the pheno summary table 

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'phenotable_provider'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	phenotable_provider_key		int		not null,
	phenotable_genotype_key		int		not null,
        provider          varchar(255)    null,
	provider_seq		int		not null,
	PRIMARY KEY(phenotable_provider_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'genotype_key' : 'create index %s on %s (phenotable_genotype_key)',
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'represents the data needed to render phenotype information on the allele and genotype detail pages ',
	Table.COLUMN : {
		'phenotable_provider_key' : 'unique key identifying this row',
		'phenotable_genotype_key' : 'key for corresponding genotype object',
		'provider' : 'provider name',
		'provider_seq' : 'sequence for the corresponding provider',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
