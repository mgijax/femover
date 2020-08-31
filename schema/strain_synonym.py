#!./python

import Table

# contains data definition information for the strain_synonym table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_synonym'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             NOT NULL,
        strain_key              int             NOT NULL,
        synonym                 text    NULL,
        synonym_type    text    NULL,
        jnum_id                 text    NULL,
        sequence_num    int             NOT NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'synonym' : 'create index %s on %s (synonym)',
        'jnum_id' : 'create index %s on %s (jnum_id)',
        }

# column name -> (related table, column in related table)
keys = {
        'strain_key' : ('strain', 'strain_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('strain_key', 'create index %s on %s (strain_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the strain flower, containing synonyms for each strain',
        Table.COLUMN : {
                'unique_key' : 'unique key for this record, no other purpose',
                'strain_key' : 'identifies the strain',
                'synonym' : 'synonym used to identify the strain',
                'synonym_type' : 'type of synonym',
                'jnum_id' : 'J: number ID of the reference which used this synonym',
                },
        Table.INDEX : {
                'strain_key' : 'clusters the data so all synonyms of a strain are stored together on disk, to aid quick retrieval',
                'synonym' : 'search by synonym',
                'jnum_id' : 'search by reference ID',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
