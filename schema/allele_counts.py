#!./python

import Table

# contains data definition information for the alleleCounts table

###--- Globals ---###

# name of this database table
tableName = 'allele_counts'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        allele_key                      int     NOT NULL,
        marker_count                    int     NULL,
        reference_count                 int     NULL,
        expression_assay_result_count   int     NULL,
        recombinase_result_count        int     NULL,
        image_count                     int     NULL,
        mutation_involves_marker_count  int     NULL,
        PRIMARY KEY(allele_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for allele flower, stores pre-computed counts for each allele',
        Table.COLUMN : {
                'allele_key' : 'identifies the allele',
                'marker_count' : 'count of markers for the allele',
                'reference_count' : 'count of references',
                'expression_assay_result_count' : 'count of expression assays involving this allele',
                'recombinase_result_count' : 'count of all_cre_cache records for this allele',
                'image_count' : 'count of images',
                'mutation_involves_marker_count' : 'count of markers associated via "mutation involves" relationships with this allele',
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
