#!./python

import Table

# contains data definition information for the mapping_insitu table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_insitu'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        insitu_key                                              int             not null,
        experiment_key                                  int     not null,
        strain                                                  text    not null,
        band                                                    text    null,
        cell_origin                                             text    null,
        karyotype_method                                text    null,
        robertsonians                                   text    null,
        metaphase_count                                 text    null,
        grains_scored                                   text    null,
        grains_on_correct_chromosome    int             null,
        grains_on_other_chromosome              int             null,
        PRIMARY KEY(insitu_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'experiment_key' : 'create index %s on %s (experiment_key)',
        }

# column name -> (related table, column in related table)
keys = {
        'experiment_key' : ('mapping_experiment', 'experiment_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains supplemental data for IN SITU mapping experiments',
        Table.COLUMN : {
                'insitu_key' : 'generated primary key for this table; no other significance',
                'experiment_key' : 'foreign key to mapping_experiment table, identifying main experiment data',
                'strain' : 'mouse strain involved in experiment',
                'band' : 'cytogenetic position on chromosome to which marker is assigned',
                'cell_origin' : 'cell type from which analyzed metaphase spreads were derived',
                'karyotype_method' : 'method used to identify chromosome bands',
                'robertsonians' : 'chromosomal aberration used to identify marker assignment',
                'metaphase_count' : '# of metaphase spreads analyzed in experiment',
                'grains_scored' : '# of grains observed in experiment',
                'grains_on_correct_chromosome' : '# of grains hybridizing to correct chromosomal location',
                'grains_on_other_chromosome' : '# of grains hybridizing to other chromosomal locations',
                },
        Table.INDEX : {
                'experiment_key' : 'quick lookup of IN SITU data for a mapping experiment by key',
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
