#!./python

import Table

# contains data definition information for the strain_grid_heading_to_term table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_grid_heading_to_term'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        heading_key             int     not null,
        term_key                int     not null,
        term_id                 text    null,
        PRIMARY KEY(heading_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'term_key' : 'create index %s on %s (term_key)',
        }

# column name -> (related table, column in related table)
keys = {
        'term_key' : ('term', 'term_key'),
        'heading_key' : ('strain_grid_heading', 'heading_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'strain_grid_heading_to_term',
        Table.COLUMN : {
                'heading_key' : 'identifies which header this term is associated with; also primary key',
                'term_key' : 'term key to be associated with the heading',
                'term_id' :  '(optional) term ID, if the term has an ID',
                },
        Table.INDEX : {
                'term_key' : 'quick retrieval of headings associated with a vocabulary term',
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
