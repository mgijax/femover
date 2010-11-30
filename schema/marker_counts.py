#!/usr/local/bin/python

import Table

# contains data definition information for the markerCounts table

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
	polymorphism_count			int	NULL,
	polymorphism_pcr_count			int	NULL,
	polymorphism_rflp_count			int	NULL,
	cdna_source_count			int	NULL,
	microarray_probeset_count		int	NULL,
	phenotype_image_count			int	NULL,
	human_disease_count			int	NULL,
	alleles_with_human_disease_count	int	NULL,
	reagents_nucleic_count			int	NULL,
	reagents_genomic_count			int	NULL,
	reagents_cdna_count			int	NULL,
	reagents_primer_pair_count		int	NULL,
	reagents_other_count			int	NULL,
	reagents_antibody_count			int	NULL,
	PRIMARY KEY(marker_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
