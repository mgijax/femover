#!./python

import Table

# contains data definition information for the expression_ht_experiment table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_experiment'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        experiment_key          int                     not null,
        primary_id                      text            null,
        source                          text            null,
        name                            text            null,
        description                     text            null,
        study_type                      text            null,
        method                          text            null,
        sample_count            int                     not null,
        is_in_atlas             int                     not null,
        PRIMARY KEY(experiment_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'primary_id' : 'create index %s on %s (primary_id)',
        'study_type' : 'create index %s on %s (study_type)',
        'name' : 'create index %s on %s (name)',
        'is_in_atlas' : 'create index %s on %s (is_in_atlas)',
        }

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains high-throughput expression experiments',
        Table.COLUMN : {
                'experiment_key' : 'unique key for an experiment, matches GXD_HTExperiment._Experiment_key in production',
                'primary_id' : 'primary accession ID for the experiment',
                'source' : 'source from which the experiment was downloaded',
                'name' : 'title of the experiment',
                'description' : 'description of the experiment',
                'study_type' : 'baseline or differential study?',
                'method' : 'type of experiment',
                'sample_count' : 'number of curated samples for the experiment',
                },
        Table.INDEX : {
                'primary_id' : 'quick lookup by primary ID',
                'study_type' : 'baseline / differential',
                'is_in_atlas' : 'flag to indicate if this experiment is in the expression atlas (1) or not (0)',
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
