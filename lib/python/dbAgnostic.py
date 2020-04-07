#!/usr/local/bin/python
# 
# provides an execute() method for executing queries against MySQL, or
#       Postgres as a source database.  Ensures that the caller does not need
#       to know which is which.

import config
import logger
import types
import dbManager

###--- Globals ---###

class DbAgnosticError(Exception):
        """
                dbAgnostic.error
        """
        
SOURCE_DB = config.SOURCE_TYPE  # either mysql, or postgres
DBM = None                      # dbManager object for postgres/mysql access

SQL_LOG_FILE = None             # file pointer; where to log SQL commands

# set up our database connectivity

if SOURCE_DB == 'postgres':
        import db
        DBM = dbManager.postgresManager (config.SOURCE_HOST, config.SOURCE_DATABASE, config.SOURCE_USER, config.SOURCE_PASSWORD)
        db.set_sqlUser(config.SOURCE_USER)
        db.set_sqlPassword(config.SOURCE_PASSWORD)
        db.set_sqlServer(config.SOURCE_HOST)
        db.set_sqlDatabase(config.SOURCE_DATABASE)
        logger.debug ('Created postgresManager')

elif SOURCE_DB == 'mysql':
        DBM = dbManager.mysqlManager (config.SOURCE_HOST,
                config.SOURCE_DATABASE, config.SOURCE_USER,
                config.SOURCE_PASSWORD)
        logger.debug ('Created mysqlManager')
else:
        raise DbAgnosticError('Unknown value for config.SOURCE_TYPE : %s' % SOURCE_DB)

class DbInitError(Exception):
        """
        Database Initialization error
        """

###--- Functions ---###

def setConnectionFE():
        global DBM
        DBM = dbManager.postgresManager (config.TARGET_HOST, config.TARGET_DATABASE, config.TARGET_USER, config.TARGET_PASSWORD)
        db.set_sqlUser(config.TARGET_USER)
        db.set_sqlPassword(config.TARGET_PASSWORD)
        db.set_sqlServer(config.TARGET_HOST)
        db.set_sqlDatabase(config.TARGET_DATABASE)
        logger.debug ('Updated postgresManager to point to FE database')
        return
        
def setSqlLogFile(filename):
        global SQL_LOG_FILE

        SQL_LOG_FILE = open(filename, 'w')
        return

def execute (cmd):
        # Purpose: execute the given SQL 'cmd' against the source database
        # Returns: two-item tuple of (columns, rows)
        #       columns - list of column names
        #       rows - list of rows, where each row is a list of values in the
        #               order corresponding to the column names in 'columns'

        if SQL_LOG_FILE:
                SQL_LOG_FILE.write(cmd)
                SQL_LOG_FILE.write('\n')
                SQL_LOG_FILE.write('||\n')
                SQL_LOG_FILE.flush()

        # if using either postgres or mysql, our dbManager object will handle
        # all necessary database interaction

        if DBM:
                return DBM.execute(cmd)
        
        raise DbInitError("dbManager not initialized")

def bcp (inputFilePointer,
        table,
        delimiter='\\t'):
        """
        BCP an inputFile (referenced by inputFilePointer)
        into table
        using delimiter as the column separator
        """
        
        if not DBM:
        
                raise DbInitError("dbManager not initialized")
                
        cursor = DBM.sharedConnection.cursor()
        
        if hasattr(cursor, 'copy_expert'):
                
                bcpCommand = "copy %s from STDIN with null as '' delimiter as E'%s' " % \
                (table, delimiter)
        
                cursor.copy_expert(bcpCommand, inputFilePointer)
                
        else:
                
                raise DbAgnosticError("BCP not supported for %s" % DBM)
        

def commit():
        if DBM:
                return DBM.commit()
        return

def columnNumber (columns, columnName):
        # find the position of 'columnName' in the list of 'columns'
        if columns and type(columns[0])==type([]):
                columns = columns[0]

        if columnName in columns:
                return columns.index(columnName)

        # Postgres returns lowercase fieldnames, so check for that

        c = columnName.lower()
        if c not in columns:
                msg = 'Column %s (%s) is not in %s' % (columnName, c, 
                        ', '.join (columns) ) 
                logger.error (msg)
                raise DbAgnosticError(msg)

        return columns.index(c)

def tuplesToLists (rows):
        # 'rows' is a list of query results.  If these results are tuples,
        # then convert them to lists instead.

        if len(rows) == 0:
                return rows

        if type(rows[0]) == list:
                return rows

        r = []
        for row in rows:
                r.append(list(row))
        return r

def mergeResultSets (cols1, rows1, cols2, rows2):
        # return a unified (cols, rows) pair, accounting for the fact
        # that column ordering in cols1 and cols2 may be different.
        # Assumes that the column names in cols1 and cols2 are
        # the same names, even if they appear in a different order.
        # Note: This function can alter rows1.

        colsMatch = True

        for c in cols1:
                if (c not in cols2):
                        raise DbAgnosticError('Item "%s" not in cols2: %s' % \
                                (c, str(cols2)))

                if (cols1.index(c) != cols2.index(c)):
                        colsMatch = False
                        break

        # easy case: the columns are already in the same order, so we
        # can just concatenate the lists

        if colsMatch:
                return (cols1, rows1 + rows2)

        # Otherwise, we'll need to ensure we re-order the values in
        # rows2 to match and append those rows to rows1.

        colOrder = []
        for c in cols1:
                if (c not in cols2):
                        raise DbAgnosticError('Item "%s" not in cols2: %s' % \
                                (c, str(cols2)))

                colOrder.append(cols2.index(c))

        for row in rows2:
                r = []
                for c in colOrder:
                        r.append(row[c])
                rows1.append(r)

        return (cols1, rows1)

def batchInsert(tableName, rows, batchSize = 1000):
        # Insert the given 'rows' into 'tableName' -- processing 'batchSize'
        # at a time.  'rows' should be a list of tuples.  Each tuple should
        # contain the values for a single row, in the proper order.
        # Note: does not check for quotes within inserted strings.

        start = 0               # where to start iterating through rows
        maxRow = len(rows)      # count of rows 

        while start < maxRow:
                end = start + batchSize

                # current slice of data rows
                values = rows[start:end]

                cmd = 'insert into %s values %s' % (tableName,
                        ','.join(map(str, values)) )
                execute(cmd)

                start = end     # prepare for next batch

        logger.debug('Added %d rows to %s' % (maxRow, tableName))
        return
