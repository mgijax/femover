# Module: Lookup.py
# Purpose: to provide a class for looking up values for various fields in
#       a database table when given a value for one field

import dbAgnostic
import logger
import cmd

###--- Classes ---###

class Lookup:
        # Is: a mapping from keys to values, where each key has its value
        #       pulled from the database and cached in memory
        # Has: a table name, a field name to search in, a field name to return,
        #       a flag indicating it's a string search, and a flag to indicate
        #       the search should be case-insensitive
        # Does: searches in a database table to find a record with a key value
        #       in one field and returns the value of another field in that
        #       record; caches those key/value pairs for efficiency of future
        #       lookups in the same process

        def __init__ (self,
                tableName,
                searchFieldName,
                returnFieldName,
                stringSearch = False,
                caseInsensitive = False,
                initClause = None                       # SQL WHERE clause to use to pre-populate with initial set of values
                ):
                self.tableName = tableName
                self.searchFieldName = searchFieldName
                self.returnFieldName = returnFieldName
                self.stringSearch = stringSearch
                self.caseInsensitive = caseInsensitive
                self.cache = {}
                self.populate(initClause)
                return

        def populate (self, initClause):
                # search the table using the given 'initClause' as a WHERE clause, allowing us to retrieve an
                # initial set of values in bulk
                
                if not initClause:
                        return
                
                cmd = 'select %s, %s from %s where %s' % (self.searchFieldName, self.returnFieldName, self.tableName, initClause)
                (cols, rows) = dbAgnostic.execute(cmd)
                keyField = dbAgnostic.columnNumber(cols, self.searchFieldName)
                valueField = dbAgnostic.columnNumber(cols, self.returnFieldName)

                for row in rows:
                        self.cache[row[keyField]] = row[valueField]
                logger.debug('Bulk added %d rows to Lookup' % len(rows))
                return
        
        def get (self, searchTerm):
                if searchTerm in self.cache:
                        return self.cache[searchTerm]

                if searchTerm == None:
                        cmd = '''select %s from %s where %s is %s'''
                        searchTerm = 'null'
                elif not self.stringSearch:
                        cmd = '''select %s from %s where %s = %s'''
                elif self.caseInsensitive:
                        cmd = '''select %s from %s where lower(%s) = '%s' '''
                        searchTerm = searchTerm.lower()
                else:
                        cmd = '''select %s from %s where %s = '%s' '''

                cmd = cmd % (self.returnFieldName, self.tableName,
                        self.searchFieldName, searchTerm)

                # only need the first match
                cmd = cmd + ' limit 1'

                (cols, rows) = dbAgnostic.execute(cmd)
                if rows:
                        match = rows[0][0]
                else:
                        match = None

                self.cache[searchTerm] = match
                return match

class AccessionLookup:
        def __init__ (self, mgiType, logicalDB = 'MGI', preferred = 1):
                self.mgiType = mgiType
                self.logicalDB = logicalDB
                self.preferred = preferred
                return
        
        def get (self, objectKey):
                # returns the first ID returned for the given objectKey, logicalDB, and preferred status

                if objectKey == None:
                        return None
                
                cmd = '''select a.accID
                        from acc_accession a, acc_logicaldb ldb, acc_mgitype t
                        where a._Object_key = %s
                                and a._LogicalDB_key = ldb._LogicalDB_key
                                and ldb.name = '%s'
                                and a._MGIType_key = t._MGIType_key
                                and t.name = '%s'
                                and a.preferred = %s
                        limit 1''' % (objectKey, self.logicalDB, self.mgiType, self.preferred)
                        
                (cols, rows) = dbAgnostic.execute(cmd)
                if rows:
                        return rows[0][0]
                return None
