#!./python

import Table

# contains data definition information for the expression_ht_experiment_sequence_num table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_experiment_sequence_num'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        experiment_key          int             not null,
        by_primary_id           int             not null,
        by_name                         int             not null,
        by_description          int             not null,
        by_study_type           int             not null,
        PRIMARY KEY(experiment_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'experiment_key' : ('expression_ht_experiment', 'experiment_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains pre-computed sequence numbers for ordering high-throughput expression experiments, one record per experiment',
        Table.COLUMN : {
                'experiment_key' : 'identifies the experiment',
                'by_primary_id' : 'sort by primary ID',
                'by_name' : 'sort by experiment name',
                'by_description' : 'sort by experiment description',
                'by_study_type' : 'sort by study type',
                },
        Table.INDEX : {},
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
                clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
