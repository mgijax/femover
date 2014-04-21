#!/usr/local/bin/python

import Table

# contains data definition information for the marker_counts table

###--- Globals ---###

# name of this database table
tableName = 'marker_counts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	marker_key				int	NOT NULL,
	reference_count				int	NULL,
	sequence_count				int	NULL,
	sequence_refseq_count			int	NULL,
	sequence_uniprot_count			int	NULL,
	allele_count				int	NULL,
	go_term_count				int	NULL,
	gxd_assay_count				int	NULL,
	gxd_result_count			int	NULL,
	gxd_literature_count			int	NULL,
	gxd_tissue_count			int	NULL,
	gxd_image_count				int	NULL,
	ortholog_count				int	NULL,
	gene_trap_count				int	NULL,
	mapping_expt_count			int	NULL,
	cdna_source_count			int	NULL,
	microarray_probeset_count		int	NULL,
	phenotype_image_count			int	NULL,
	human_disease_count			int	NULL,
	alleles_with_human_disease_count	int	NULL,
	antibody_count				int	NULL,
	imsr_count				int	NULL,
	PRIMARY KEY(marker_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'marker_key' : ('marker', 'marker_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the marker flower, storing pre-computed counts for each marker',
	Table.COLUMN : {
		'marker_key' : 'identifies the marker',
		'reference_count' : 'count of related references',
		'sequence_count' : 'count of related sequences',
		'sequence_refseq_count' : 'count of RefSeq sequences',
		'sequence_uniprot_count' : 'count of UniProt sequences',
		'allele_count' : 'count of all alleles',
		'go_term_count' : 'count of GO terms annotated to this marker',
		'gxd_assay_count' : 'count of expression assays',
		'gxd_result_count' : 'count of expression results',
		'gxd_literature_count' : 'count of expression literature index entries',
		'gxd_tissue_count' : 'count of tissues in which expression was studied',
		'gxd_image_count' : 'count of expression images',
		'ortholog_count' : 'count of orthologous markers',
		'gene_trap_count' : 'count of gene trap insertions',
		'mapping_expt_count' : 'count of mapping experiments',
		'cdna_source_count' : 'count of cDNA sources',
		'microarray_probeset_count' : 'count of microarray probe sets',
		'phenotype_image_count' : 'count of phenotype images for alleles of this marker',
		'human_disease_count' : 'count of human diseases for which this marker has mouse models',
		'alleles_with_human_disease_count' : 'count of alleles of this marker which are mouse models of a human disease',
		'antibody_count' : 'count of antibodies',
		'imsr_count' : 'count of lines/mice in IMSR',
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
