# Name: SanityChecks.py
# Purpose: include some basic sanity checks that can be run against a front-end database
#   to verify that it's in a good state at the end of the femover run
# Note: no changes needed for python 3.7

import sys
import os
import logger

###--- Constants ---###

# not real tables
NOT_TABLES = [ 'Table', 'template_table', 'config', 'Configuration', ]

# tables where having zero rows is okay
ZERO_ROWS_OKAY = [ 'marker_mrm_property' ]

###--- Classes ---###

class SanityCheckSet:
    def __init__ (self, dbm):
        # pass in a dbManager configured for the front-end database
        #    and a file pointer that's been opened for reading (for reporting) 
        self.dbm = dbm
        self.checks = []
        return
    
    def addCheck (self, sc):
        self.checks.append(sc)
        return
    
    def isValid (self):
        passedCount = 0
        failedCount = 0
        
        for check in self.checks:
            if check.isValid(self.dbm):
                passedCount = passedCount + 1
            else:
                failedCount = failedCount + 1
                
        logger.info('Sanity Check Report:')
        logger.info('  passed: %d' % passedCount)
        if failedCount > 0:
            logger.info('  failed: %d' % failedCount)
                
        return failedCount == 0
    
class SanityCheck:
    def __init__ (self):
        return
    
    def isValid (self, dbm):
        # pass in a dbManager configured for the front-end database
        #    and a file pointer that's been opened for reading (for reporting)
        return True

class TableNotEmptyCheck (SanityCheck):
    # simple check to test if a database table with the given name has any rows
    # (If no rows, then the sanity check fails.)
    def __init__ (self, tableName):
        self.tableName = tableName
        return
    
    def isValid (self, dbm):
        (cols, rows) = dbm.execute('select * from %s limit 1' % self.tableName)
        if len(rows) > 0:
            return True

        logger.info('Sanity Check Failed: table %s has no rows' % self.tableName)
        return False

class TablesNotEmptySet (SanityCheckSet):
    def __init__ (self, dbm):
        SanityCheckSet.__init__ (self, dbm)
        
        for filename in os.listdir('../schema'):
            table = filename.replace('.pyc', '').replace('.py', '')
            if (table not in NOT_TABLES) and (table not in ZERO_ROWS_OKAY):
                self.addCheck(TableNotEmptyCheck(table))
        return
    
###--- Functions ---###

def databaseIsValid (dbm):
    scSet = TablesNotEmptySet(dbm)
    return scSet.isValid()
