#!./python

import Table

# contains data definition information for the allele_synonym table

###--- Globals ---###

# name of this database table
tableName = 'allele_synonym'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        allele_key              int             not null,
        synonym                 text    null,
        synonym_type            text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

clusteredIndex = ('allele_key', 'create index %s on %s (allele_key)')

comments = {
        Table.TABLE : 'petal table for the allele flower, containing synonyms for alleles',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this record, no other purpose',
                'allele_key' : 'identifies the allele',
                'synonym' : 'text of the synonym',
                'synonym_type' : 'type of synonym',
                },
        Table.INDEX : {
                'allele_key' : 'clusters data so all synonyms for an allele are stored together on disk, to aid quick retrieval',
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
