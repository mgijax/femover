#!./python

import Table

# contains data definition information for the mapping_fish table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_fish'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        fish_key                        int             not null,
        experiment_key          int             not null,
        strain                          text    not null,
        band                            text    null,
        cell_origin                     text    null,
        karyotype_method        text    null,
        robertsonians           text    null,
        metaphase_count         text    null,
        single_signal_count     text    null,
        double_signal_count     text    null,
        label                           text    null,
        PRIMARY KEY(fish_key))''' % tableName

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
        Table.TABLE : 'contains supplemental data for FISH mapping experiments',
        Table.COLUMN : {
                'fish_key' : 'generated primary key for this table; no other significance',
                'experiment_key' : 'foreign key to mapping_experiment table, identifying main experiment data',
                'strain' : 'mouse strain involved in experiment',
                'band' : 'cytogenetic position on chromosome to which marker is assigned',
                'cell_origin' : 'cell type from which analyzed metaphase spreads were derived',
                'karyotype_method' : 'method used to identify chromosome bands',
                'robertsonians' : 'chromosomal aberration used to identify marker assignment',
                'metaphase_count' : '# of metaphase spreads analyzed in experiment',
                'single_signal_count' : '# of single flourescent signals indicating presence of marker',
                'double_signal_count' : '# of double flourescent signals indicating presence of marker',
                'label' : 'type of fluorescence on probe',
                },
        Table.INDEX : {
                'experiment_key' : 'quick lookup of FISH data for a mapping experiment by key',
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
