#!./python

import Table

# contains data definition information for the marker_counts table

###--- Globals ---###

# name of this database table
tableName = 'marker_counts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        marker_key                              int     NOT NULL,
        reference_count                         int     NULL,
        disease_relevant_reference_count        int     NULL,
        go_reference_count                      int     NULL,
        phenotype_reference_count               int     NULL,
        sequence_count                          int     NULL,
        sequence_refseq_count                   int     NULL,
        sequence_uniprot_count                  int     NULL,
        allele_count                            int     NULL,
        rec_allele_count                        int     NULL,
        go_term_count                           int     NULL,
        gxd_assay_count                         int     NULL,
        gxd_result_count                        int     NULL,
        gxd_literature_count                    int     NULL,
        gxd_tissue_count                        int     NULL,
        gxd_image_count                         int     NULL,
        ortholog_count                          int     NULL,
        gene_trap_count                         int     NULL,
        mapping_expt_count                      int     NULL,
        cdna_source_count                       int     NULL,
        microarray_probeset_count               int     NULL,
        phenotype_image_count                   int     NULL,
        human_disease_count                     int     NULL,
        alleles_with_human_disease_count        int     NULL,
        antibody_count                          int     NULL,
        imsr_count                              int     NULL,
        mutation_involves_count                 int     NULL,
        mp_annotation_count                     int     NULL,
        mp_allele_count                         int     NULL,
        mp_background_count                     int     NULL,
        mp_multigenic_annotation_count          int     NULL,
        PRIMARY KEY(marker_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'gxd_literature_count' : 'create index %s on %s (gxd_literature_count)',
}

keys = { 'marker_key' : ('marker', 'marker_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table in the marker flower, storing pre-computed counts for each marker',
        Table.COLUMN : {
                'marker_key' : 'identifies the marker',
                'reference_count' : 'count of related references',
                'disease_relevant_reference_count' : 'count of related references which are related to disease annotations',
                'go_reference_count' : 'count of references for GO annotations for this marker',
                'phenotype_reference_count' : 'count of references indexed to alleles of this marker',
                'sequence_count' : 'count of related sequences',
                'sequence_refseq_count' : 'count of RefSeq sequences',
                'sequence_uniprot_count' : 'count of UniProt sequences',
                'allele_count' : 'count of all alleles',
                'rec_allele_count' : 'count of recombinase alleles driven by this marker or any Alliance Direct orthologs having activity data',
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
                'mutation_involves_count' : 'count of alleles with this marker in a mutation-involves relationship',
                'mp_annotation_count' : 'count of MP annotations which roll up to the marker',
                'mp_allele_count' : 'count of non-wild-type alleles which contribute to those annotations',
                'mp_background_count' : 'number of distinct background strains for genotypes with rolled up annotations',
                'mp_multigenic_annotation_count' : 'count of annotations which did not roll up to this marker due to multigenic genotypes',
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
