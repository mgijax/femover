#!/usr/local/bin/python

import Table
import logger

# contains data definition information for the database info table

###--- Classes ---###

class databaseInfoTable (Table.Table):
        def grantSelect (self):
                cmd = '''grant select on database_info to public'''
                dbm = Table.DBM
                dbm.execute(cmd)
                dbm.commit()
                logger.debug ('Opened permissions on database_info')
                return

        def setInfo (self, name, value):
                dbm = Table.DBM
                cols, rows = dbm.execute ('''select unique_key
                        from database_info
                        where name = '%s' ''' % name)

                if len(rows) > 0:
                        nextKey = rows[0][0]
                        cmd = '''update database_info set value = '%s'
                                where unique_key = %d''' % (value, nextKey)
                else:
                        cols, rows = dbm.execute ('''select max(unique_key)
                                from database_info''')
                        if (len(rows) > 0) and (rows[0][0] != None):
                                nextKey = rows[0][0] + 1
                        else:
                                nextKey = 1

                        cmd = '''insert into database_info (unique_key, name,
                                        value) values (%d, '%s', '%s')''' % (
                                                nextKey, name, value)
                dbm.execute(cmd)
                dbm.commit()
#               logger.debug ('Added %s = %s as key %d' % (name, value,
#                       nextKey))
                return

###--- Globals ---###

# name of this database table
tableName = 'database_info'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             not null,
        name            text    not null,
        value           text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

comments = {
        Table.TABLE : 'standalone table; contains data about when this database was last updated, what its source database was, and other metadata.  Each record contains one property name and its associated value.',
        Table.COLUMN : {
            'unique_key' : 'uniquely identifies records in this table',
            'name' : 'name of the property being described',
            'value' : 'value of the property, as a string',
                },
        }

# global instance of this Table object
table = databaseInfoTable (tableName, createStatement, indexes, {}, comments)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
