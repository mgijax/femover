#!./python

import Table

# contains data definition information for the term_emap table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_emap'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        term_key                int     not null,
        default_parent_key      int     null,
        start_stage             int     null,
        end_stage               int     null,
        stage                   int     null,
        emapa_term_key          int     null,
        PRIMARY KEY(term_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'emapa_term_key' : 'create index %s on %s (emapa_term_key)',
        'start_stage' : 'create index %s on %s (start_stage)',
        'end_stage' : 'create index %s on %s (end_stage)',
        'stage' : 'create index %s on %s (stage)',
        }

# column name -> (related table, column in related table)
keys = {
        'term_key'              : ('term', 'term_key'),
        'default_parent_key'    : ('term', 'term_key'),
        'emapa_term_key'        : ('term', 'term_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains data related specifically to terms in the EMAPA and EMAPS vocabularies',
        Table.COLUMN : {
                'term_key' : 'foreign key to the term table',
                'default_parent_key' : 'identifies the parent term to open by default in the browser tree view',
                'start_stage' : 'for EMAPA terms: identifies the Theiler stage when the structure first appears',
                'end_stage' : 'for EMAPA terms: identifies the Theiler stage when the strucure ceases to exist',
                'stage' : 'for EMAPS terms: identifies the Theiler stage for this particular term',
                'emapa_term_key' : 'for EMAPS terms: identifies the EMAPA term used to generate this EMAPS term',
                },
        Table.INDEX : {
                'emapa_term_key' : 'quick access to all EMAPS terms generated from a given EMAPA term',
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
