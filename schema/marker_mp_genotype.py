#!/usr/local/bin/python

import Table

# contains data definition information for the marker_mp_genotype table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_mp_genotype'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
    mp_genotype_key        int        not null,
    marker_key        int        not null,
    is_multigenic        int        not null,
    allele_pairs        text        null,
    strain            text        null,
    genotype_id        text        null,
    sequence_num        int        not null,
    PRIMARY KEY(mp_genotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
    'multigenic' : 'create index %s on %s (is_multigenic)',
    }

# column name -> (related table, column in related table)
keys = {
    'marker_key' : ('marker', 'marker_key'),
    }

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
    Table.TABLE : 'stores mapping from a marker to the allele pairs and background strains for which it has phenotype annotations, including a flag for those which are multigenic (and hence, not rolled-up)',
    Table.COLUMN : {
        'mp_genotype_key' : 'generated primary key for this table',
        'marker_key' : 'foreign key to marker table, indicating the marker with the annotation',
        'is_multigenic' : '0 if this annotation was rolled-up to the marker, 1 if it was multigenic and could not be rolled-up',
        'allele_pairs' : 'output string for the allele pairs, containing MGI markup for alleles',
        'strain' : 'name of the background strain on which the alleles were placed',
        'genotype_id' : 'primary ID of the genotype',
        'sequence_num' : 'sequence number for ordering genotypes of a marker',
        },
    Table.INDEX : {
        'multigenic' : 'quick access to only multigenic or rolled-up annotations for a marker',
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
