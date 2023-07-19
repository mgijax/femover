#!./python

import Table

# contains data definition information for the expression_ht_sample table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_sample'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        sample_key              int     not null,
        experiment_key          int     not null,
        name                    text    null,
        genotype_key            int     not null,
        organism                text    null,
        sex                     text    null,
        age                     text    null,
        age_min                 float   null,
        age_max                 float   null,
        emapa_key               int     null,
        theiler_stage           text    null,
        celltype_key            int     null,
        relevancy               text    not null,
        sequence_num            int     not null,
        PRIMARY KEY(sample_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'genotype_key' : 'create index %s on %s (genotype_key)',
        'emapa_key' : 'create index %s on %s (emapa_key)',
        'celltype_key' : 'create index %s on %s (celltype_key)',
        }

# column name -> (related table, column in related table)
keys = {
        'experiment_key' : ('expression_ht_experiment', 'experiment_key'),
        'genotype_key' : ('genotype', 'genotype_key'),
        'emapa_key' : ('term', 'term_key'),
        'celltype_key' : ('term', 'term_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('experiment_key', 'create index %s on %s (experiment_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'base table for samples associated with high-throughput expressione experiments',
        Table.COLUMN : {
                'sample_key' : 'uniquely identifies this sample; matches GXD_HTSample._Sample_key in production db',
                'experiment_key' : 'identifies the experiment with which the sample is associated',
                'name' : 'name of the sample',
                'genotype_key' : 'identifies the genotype for the sample',
                'organism' : 'organism of the sample',
                'sex' : 'sex of the sample',
                'age' : 'age of the sample (as typed)',
                'age_min' : 'minimum age of the sample, computed as DPC',
                'age_max' : 'maxiumum age of the sample, computed as DPC',
                'emapa_key' : 'structure of the sample',
                'theiler_stage' : 'Theiler stage of the sample, if available',
                'celltype_key' : 'cell type of the sample',
                'relevancy' : 'is this sample relevant for GXD purposes?',
                'sequence_num' : 'femover-computed sequence number; used for ordering the samples of each experiment',
                },
        Table.INDEX : {
                'emapa_key' : 'identifies the structure of the sample',
                'celltype_key' : 'identifies the cell type of the sample',
                'genotype_key' : 'identifies the genotype of the sample',
                'experiment_key' : 'clustered index, brings together all samples for an experiment for quick access',
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
