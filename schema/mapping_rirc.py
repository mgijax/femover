#!./python

import Table

# contains data definition information for the mapping_rirc table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_rirc'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        rirc_key                int             not null,
        experiment_key  int             not null,
        designation             text    not null,
        abbreviation_1  text    not null,
        abbreviation_2  text    not null,
        strain_1                text    not null,
        strain_2                text    not null,
        PRIMARY KEY(rirc_key))''' % tableName

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
        Table.TABLE : 'contains basic RI/RC for mapping experiments, one record per experiment record',
        Table.COLUMN : {
                'rirc_key' : 'generated primary key for this table; no other significance',
                'experiment_key' : 'foreign key to mapping_experiment table, identifying the main experiment record',
                'designation' : 'label for the RI set',
                'abbreviation_1' : 'strain 1 abbreviation',
                'abbreviation_2' : 'strain 2 abbreviation',
                'strain_1' : 'name for strain 1',
                'strain_2' : 'name for strain 2',
                },
        Table.INDEX : {
                'experiment_key' : 'quick access to RI/RC data by experiment_key',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
