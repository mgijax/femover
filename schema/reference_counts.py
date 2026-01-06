#!./python

import Table

# contains data definition information for the reference_counts table

###--- Globals ---###

# name of this database table
tableName = 'reference_counts'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        reference_key           int     NOT NULL,
        marker_count            int     NULL,
        probe_count             int     NULL,
        antibody_count          int     NULL,
        mapping_expt_count      int     NULL,
        gxd_index_count         int     NULL,
        gxd_result_count        int     NULL,
        gxd_structure_count     int     NULL,
        gxd_assay_count         int     NULL,
        gxd_htexp_count         int     NULL,
        allele_count            int     NULL,
        sequence_count          int     NULL,
        go_annotation_count     int     NULL,
        strain_count            int     NULL,
        disease_model_count     int     NULL,
        PRIMARY KEY(reference_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'reference_key' : ('reference', 'reference_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the reference flower; contains pre-computed counts for each reference',
        Table.COLUMN : {
                'reference_key' : 'identifies the reference',
                'marker_count' : 'count of markers (genes, etc.)',
                'probe_count' : 'count of probes',
                'antibody_count' : 'count of antibodies',
                'mapping_expt_count' : 'count of mapping experiments',
                'gxd_index_count' : 'count of markers indexed for expression data',
                'gxd_result_count' : 'count of expression results',
                'gxd_structure_count' : 'count of anatomical structures annotated for expression data',
                'gxd_assay_count' : 'count of expression assays',
                'gxd_htexp_count' : 'count of high-throughput expression experiments',
                'allele_count' : 'count of alleles',
                'sequence_count' : 'count of sequences',
                'strain_count' : 'count of mouse strains',
                'disease_model_count' : 'count of genotypes with disease model annoations',
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
