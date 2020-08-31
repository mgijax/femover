#!./python

import Table

# contains data definition information for the genotype_disease table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype_disease'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        genotype_disease_key    int             not null,
        genotype_key    int             not null,
        is_heading              int             not null,
        is_not                  int             not null,
        term                    text    null,
        term_key                        int             not null,
        term_id                 text    null,
        reference_count         int             not null,
        has_footnote            int             not null,
        sequence_num            int             not null,
        PRIMARY KEY(genotype_disease_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {'genotype_key_idx' : 'create index %s on %s (genotype_key)'}

keys = { 'genotype_key' : ('genotype', 'genotype_key')}

# index used to cluster data in the table
clusteredIndex = ('genotype',
        'create index %s on %s (genotype_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table in the genotype flower, identifying diseases associated with allele/genotype pairs',
        Table.COLUMN : {
                'genotype_disease_key' : 'unique key identifying this record',
                'genotype_key' : 'identifies the genotype',
                'is_heading' : '1 if this is a record defining a table heading, rather than a disease, 0 if it is a disease annotation',
                'is_not' : '1 if this is a NOT annotation (the researcher expected to find the disease in this genotype, but did not find it), 0 if it is a positive annotation (the disease was found in this genotype)',
                'term' : 'the disease that was found (or the heading term, if is_heading = 1)',
                'term_key' : 'preferred term key for the disease term',
                'term_id' : 'preferred accession ID for the disease term',
                'reference_count' : 'number of references supporting this annotation',
                'has_footnote' : '1 if there is a footnote for this annotation, 0 if not',
                'sequence_num' : 'for ordering disease annotations for each allele/genotype pair',
                },
        Table.INDEX : {
                'genotype' : 'clusters data by allele/genotype pair, to aid efficient retrieval of their diseases',
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
