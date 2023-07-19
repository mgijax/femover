#!./python

import Table

# contains data definition information for the expression_result_sequence_num
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_result_sequence_num'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        result_key              int             not null,
        by_assay_type           int             not null,
        by_gene_symbol          int             not null,
        by_age                  int             not null,
        by_structure            int             not null,
        by_expressed            int             not null,
        by_mutant_alleles       int             not null,
        by_reference            int             not null,
        PRIMARY KEY(result_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'result_key' : ('expression_result_summary', 'result_key'),
        }

comments = {
        Table.TABLE : 'petal table for expression result flower; contains various pre-computed sorts to allow for easy ordering of expression results.  There will be exactly one row in this table for each row in expression_result_summary.',
        Table.COLUMN : {
            'result_key' : 'foreign key to expression_result_summary; specifies the result to which this row corresponds',
            'by_assay_type' : 'sort by assay tpye, symbol, age min, age max, structure, stage, and whether expressed',
            'by_gene_symbol' : 'sort by symbol, assay type, age min, age max, structure, stage, and whether expressed',
            'by_age' : 'sort by age min, age max, structure, stage, whether expressed, symbol, and assay type',
            'by_structure' : 'sort by structure, stage, age min, age max, whether expressed, symbol, and assay type',
            'by_expressed' : 'sort by whether expressed, symbol, assay type, age min, age max, structure, and stage',
            'by_mutant_alleles' : 'sort by mutant allele symbols, gene symbol, assay type, age min, age max, structure, and stage',
            'by_reference' : 'sort by reference citation, symbol, assay type, age min, age max, structure, and stage',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
