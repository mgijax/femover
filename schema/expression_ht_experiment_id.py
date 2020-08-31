#!./python

import Table

# contains data definition information for the expression_ht_experiment_id table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_experiment_id'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        experiment_key  int             not null,
        logical_db              text    null,
        acc_id                  text    null,
        preferred               int             not null,
        private                 int             not null,
        sequence_num    int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'acc_id' : 'create index %s on %s (acc_id)',
        'logical_db' : 'create index %s on %s (logical_db)',
        }

# column name -> (related table, column in related table)
keys = {
        'experiment_key' : ('expression_ht_experiment', 'experiment_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('experiment_key', 'create index %s on %s (experiment_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'primary and secondary IDs for high-throughput expression experiments',
        Table.COLUMN : {
                'unique_key' : 'unique identifier for this record, no other significance',
                'experiment_key' : 'identifies the experiment',
                'logical_db' : 'identifies the entity that assigned the ID',
                'acc_id' : 'the accession ID itself',
                'preferred' : '1 if this is the preferred ID assigned by the logical_db for this experiment, 0 if it is a secondary ID',
                'private' : '1 if this ID should be considered private, 0 if public',
                'sequence_num' : 'for ordering accession IDs for display',
                },
        Table.INDEX : {
                'experiment_key' : 'clustered index, bringing together all IDs for an experiment together for fast retrieval',
                'acc_id' : 'quick lookup of an experiment by ID',
                'logical_db' : 'helps queries that include logical_db in the WHERE section',
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
