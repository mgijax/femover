#!./python

import Table

# contains data definition information for the marker_grid_heading_to_term table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_grid_heading_to_term'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int     not null,
        heading_key             int     not null,
        term_key                int     not null,
        term_id                 text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'term_key' : 'create index %s on %s (term_key)',
        }

# column name -> (related table, column in related table)
keys = {
        'term_key' : ('term', 'term_key'),
        'heading_key' : ('marker_grid_heading', 'heading_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('heading_key', 'create index %s on %s (heading_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'marker_grid_heading_to_term',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies a row in this table, no other purpose',
                'heading_key' : 'identifies which header this term is associated with',
                'term_key' : 'term key to be associated with the heading',
                'term_id' :  '(optional) term ID, if the term has an ID',
                },
        Table.INDEX : {
                'term_key' : 'quick retrieval of headings associated with a vocabulary term',
                'heading_key' : 'used to cluster records so all terms for a given header will be stored together',
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
