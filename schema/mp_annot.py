#!./python

import Table

# contains data definition information for the annotation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mp_annot'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        mp_annotation_key               int             not null,
        mp_term_key             int             not null,
        call            int             not null,
        sex             text    null,
        annotation_seq                  int     null,
        PRIMARY KEY(mp_annotation_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'term_key' : 'create index %s on %s (mp_term_key)',
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'represents the data needed to render phenotype information on the allele and genotype detail pages ',
        Table.COLUMN : {
                'mp_annotation_key' : 'unique key identifying this annotation (does not correspond to _Annot_key in mgd)',
                'mp_term_key' : 'key for the  parent table',
                'call' : 'call (1=Y or 0=N)',
                'sex' : 'male/female/unknown',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
                clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
