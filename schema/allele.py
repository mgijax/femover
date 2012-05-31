#!/usr/local/bin/python

import Table

# contains data definition information for the allele table

###--- Globals ---###

# name of this database table
tableName = 'allele'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_key		int		not null,
	symbol			varchar(60)	null,
	name			varchar(255)	null,
	only_allele_symbol	varchar(60)	null,
	gene_name		varchar(255)	null,
	primary_id		varchar(30)	null,
	logical_db		varchar(80)	null,
	allele_type		varchar(255)	null,
	allele_subtype		varchar(255)	null,
	is_recombinase		int		not null,
	is_wild_type		int		not null,
	driver			varchar(50)	null,
	inducible_note		varchar(255)	null,
	molecular_description	varchar(2048)	null,
	strain			varchar(255)	null,
	strain_type		varchar(255)	null,
	inheritance_mode	varchar(255)	null,
	holder			varchar(30)	null,
	company_id		varchar(30)	null,
	transmission_type	varchar(30)	null,
	transmission_phrase	varchar(255)	null,
	primary_image_key	int		null,
	has_disease_model	int		null,
	PRIMARY KEY(allele_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'symbol' : 'create index %s on %s (symbol)',
	'primary_id' : 'create index %s on %s (primary_id)',
	}

keys = {
	'primary_image_key' : ('image', 'image_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the allele flower, containing basic data for alleles',
	Table.COLUMN : {
		'allele_key' : 'unique key identifying the allele, same as _Allele_key in mgd',
		'symbol' : 'gene symbol and allele symbol, as gene<allele> where the part in brackets should be superscripted',
		'name' : 'allele name',
		'only_allele_symbol' : 'only the superscripted piece of the symbol',
		'gene_name' : 'name of the gene for this allele',
		'primary_id' : 'preferred accession ID for the allele',
		'logical_db' : 'logical database assigning the ID',
		'allele_type' : 'type of allele',
		'allele_subtype' : 'subtype of allele',
		'is_recombinase' : '1 if this is a recombinase allele, 0 if not',
		'is_wild_type' : '1 if this is a wild-type allele, 0 if not',
		'driver' : 'driver for this allele, if it is a recombinase allele',
		'inducible_note' : 'inducible note',
		'molecular_description' : 'molecular description',
		'strain' : 'strain which is the source of the allele',
		'strain_type' : 'type of strain',
		'inheritance_mode' : 'how is this allele passed on?',
		'holder' : 'who has the allele',
		'company_id' : 'ID of the company with the allele',
		'transmission_type' : 'method of transmission',
		'transmission_phrase' : 'phrase to use in describing transmission type on allele detail page',
		'primary_image_key' : 'identifies the primary image for the allele',
		'has_disease_model' : '1 if this allele is a model for at least one human disease, 0 if not',
		},
	Table.INDEX : {
		'symbol' : 'look up alleles by symbol',
		'primary_id' : 'look up alleles by primary accession ID',
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
