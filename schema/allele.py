#!/usr/local/bin/python

import Table

# contains data definition information for the allele table

###--- Globals ---###

# name of this database table
tableName = 'allele'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_key		int		not null,
	symbol			text	null,
	name			text	null,
	only_allele_symbol	text	null,
	gene_name		text	null,
	primary_id		text	null,
	logical_db		text	null,
	allele_type		text	null,
	allele_subtype		text	null,
	chromosome		text		null,
	collection		text 	null,
	is_recombinase		int		not null,
	is_wild_type		int		not null,
	is_mixed		int		not null,
	driver			text	null,
	driver_key		int		null,
	inducible_note		text	null,
	molecular_description	text		null,
	strain			text	null,
	strain_type		text	null,
	strain_id		text	null,
	inheritance_mode	text	null,
	holder			text	null,
	company_id		text	null,
	transmission_type	text	null,
	transmission_phrase	text	null,
	primary_image_key	int		null,
	has_disease_model	int		null,
	repository		text		null,
	jrs_id			text		null,
	PRIMARY KEY(allele_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'symbol' : 'create index %s on %s (symbol)',
	'primary_id' : 'create index %s on %s (primary_id)',
	'driver_key' : 'create index %s on %s (driver_key)',
	}

keys = {
	'primary_image_key' : ('image', 'image_key'),
	'driver_key' : ('marker', 'marker_key'),
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
		'chromosome' : 'chromosome of allele',
		'collection' : 'allele collection',
		'is_recombinase' : '1 if this is a recombinase allele, 0 if not',
		'is_wild_type' : '1 if this is a wild-type allele, 0 if not',
		'driver' : 'driver for this allele, if it is a recombinase allele',
		'driver_key' : 'marker key for the driver for this allele, if it is a recombinase allele',
		'inducible_note' : 'inducible note',
		'molecular_description' : 'molecular description',
		'strain' : 'strain which is the source of the allele',
		'strain_type' : 'type of strain',
		'strain_id' : 'ID for linking to strain detail page (for strains moved to fe database)',
		'inheritance_mode' : 'how is this allele passed on?',
		'holder' : 'who has the allele',
		'company_id' : 'ID of the company with the allele',
		'transmission_type' : 'method of transmission',
		'transmission_phrase' : 'phrase to use in describing transmission type on allele detail page',
		'primary_image_key' : 'identifies the primary image for the allele',
		'has_disease_model' : '1 if this allele is a model for at least one human disease, 0 if not',
		'repository' : 'the repository from which this knockout is available',
		'jrs_id' : 'the Jax Registry ID (JRS) of the knockout',
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
