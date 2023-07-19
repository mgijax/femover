#!./python

import Table

# contains data definition information for the expression_ht_experiment_property table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_experiment_property'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        property_key            int             not null,
        experiment_key          int             not null,
        name                            text    not null,
        value                           text    null,
        sequence_num            int             not null,
        PRIMARY KEY(property_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'experiment_key' : ('expression_ht_experiment', 'experiment_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('experiment_key', 'create index %s on %s (experiment_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains name:value properties for high-throughput expression experiments',
        Table.COLUMN : {
                'property_key' : 'unique key for this database record, no other significance',
                'experiment_key' : 'identifies the experiment',
                'name' : 'name of the property',
                'value' : 'value for the property',
                'sequence_num' : 'can be used to order the values for a property, in case of multiples',
                },
        Table.INDEX : {
                'experiment_key' : 'clustered index to bring properties for an experiment together for fast retrieval',
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
