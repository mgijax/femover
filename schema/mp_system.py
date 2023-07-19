#!./python

import Table

# contains MP system information for the genotype pheno summary  page

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mp_system'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        mp_system_key           int             not null,
        genotype_key            int             not null,
        system          text    null,
        system_seq                      int     null,
        PRIMARY KEY(mp_system_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
'genotype_key' : 'create index %s on %s (genotype_key)',

}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'represents the data needed to render phenotype information on the genotype detail pages ',
        Table.COLUMN : {
                'mp_system_key' : 'unique key identifying this row',
                'genotype_key' : 'key for the allele',
                'system' : 'display name for an MP header term',
                'system_seq' : 'system sort order',
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
