#!./python

import Table

# contains data definition information for the term_sequence_num table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_sequence_num'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        term_key                int     not null,
        by_default              int     not null,
        by_dfs                  int     not null,
        by_vocab_dag_term       int     not null,
        PRIMARY KEY(term_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'term_key' : ('term', 'term_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('term_key', 'create index %s on %s (term_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'stores pre-computed values used to sort terms various ways efficiently; has one row for each row in the term table',
        Table.COLUMN : {
                'term_key' : 'foreign key to term table, identifies which term we are considering',
                'by_default' : 'default sort; if term has a sequence_num defined in mgd, use that; if not, sort lowercase version alphabetically',
                'by_dfs' : 'sort by vocab, then terms within the DAGs of the vocab by a depth-first traversal',
                'by_vocab_dag_term' : 'sort by vocab, then by DAG, then by term within the DAG',
                },
        Table.INDEX : {
                'term_key' : 'cluster data by term key',
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
