#!./python

import Table

# contains data definition information for the expression_ht_experiment_to_reference table

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_experiment_to_reference'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int     NOT NULL,
        experiment_key    int     NOT NULL,
        reference_key   int     NOT NULL,
        qualifier       text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'reference_key' : 'create index %s on %s (reference_key)',
        }

keys = {
        'experiment_key' : ('expression_ht_experiment', 'experiment_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('experiment_key', 'create index %s on %s (experiment_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between the expression_ht_experiment and reference flowers',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record, no other purpose',
                'experiment_key' : 'identifies the ht experiment',
                'reference_key' : 'identifies the reference',
                'qualifier' : 'qualifier describing the association (often null)',
                },
        Table.INDEX : {
                'reference_key' : 'look up all experiments for a reference',
                'experiment_key' : 'clusters data so all references for an experiment are stored together on disk, to aid quick retrieval',
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
