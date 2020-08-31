#!/usr/local/bin/python
# 
# base table gatherer for the Mover suite -- queries, MySQL, or
#       Postgres as a source database, and writes text files.

import os
import tempfile
import sys

import config
import logger
import types
import OutputFile
import top
import Checksum
import dbAgnostic

###--- Globals ---###

error = 'Gatherer.error'        # exception raised by this module
AUTO = OutputFile.AUTO          # special fieldname for auto-incremented field
SOURCE_DB = config.SOURCE_TYPE  # either, mysql, or postgres

# cache of terms already looked up
resolveCache = {}               # resolveCache[table][keyField][key] = term

# top.Process object for the currently executing script
myProcess = top.getMyProcess()

###--- Functions ---###

def myMemory():
        # Return as a string the current memory used by the current process

        #myProcess.measure()
        #return top.displayMemory(myProcess.getLatestMemoryUsed())
        return "disabled"

def resolve (key,               # integer; key value to look up
        table = "voc_term",     # string; table in which to look up key
        keyField = "_Term_key", # string; field in which to look up key
        stringField = "term"    # string; field with text correponding to key
        ):
        # Purpose: to look up the string associated with 'key' in 'table'.
        #       'keyField' specifies the fieldname containing the 'key', while
        #       'stringField' identifies the field with its corresponding
        #       string value
        # Returns: string (or None if no matching key can be found)
        # Assumes: we can query our source database
        # Modifies: adds entries to global 'resolveCache' to cache values as
        #       they are looked up
        # Throws: propagates any exceptions from dbAgnostic.execute()

        global resolveCache

        tableDict = None
        keyDict = None
        fieldDict = None
        term = None

        if table in resolveCache:
                tableDict = resolveCache[table]
                if keyField in tableDict:
                        keyDict = tableDict[keyField]
                        if stringField in keyDict:
                                fieldDict = tableDict[keyField][stringField]
                                if key in fieldDict:
                                        return fieldDict[key]

        cmd = 'select %s from %s where %s = %d' % (stringField, table,
                keyField, key)

        columns, rows = dbAgnostic.execute(cmd)
        if len(rows) > 0:
                term = rows[0][0]

        if tableDict == None:
                tableDict = {}
                resolveCache[table] = tableDict
                
        if keyDict == None:
                keyDict = {}
                tableDict[keyField] = keyDict

        if fieldDict == None:
                fieldDict = {}
                tableDict[keyField][stringField] = fieldDict

        fieldDict[key] = term
        return term

def columnNumber (columns, columnName):
        return dbAgnostic.columnNumber (columns, columnName)

def executeQueries (cmds):
        # Purpose: to issue the queries and collect the results
        # Returns: list of lists, each with (columns, rows) for a query
        # Assumes: nothing
        # Modifies: queries the database
        # Throws: propagates all exceptions

        if not cmds:
                raise Exception('%s: No SQL commands given to executeQueries()' % error)

        if type(cmds) == bytes:
                cmds = [ cmds ]
        i = 0
        results = []
        for cmd in cmds:
                results.append (dbAgnostic.execute (cmd))
                count = 0
                if results[-1][1]:
                        count = len(results[-1][1])
                logger.debug ('Finished query %d (%d results)' % (i, count))
                logger.debug ('RAM used : %s' % myMemory())
                i = i + 1
        return results

def main (
        gatherer        # Gatherer object; object to use in gathering data
        ):
        # Purpose: to serve as the main program for any Gathering subclass
        # Returns: nothing
        # Assumes: nothing
        # Modifies: queries the database, writes to the file system
        # Throws: propagates all exceptions
        # Notes: Inspects the command-line to determine how to proceed.  If
        #       there were no command-line arguments, then we do a full
        #       gathering operation.  If there are command-line arguments,
        #       then we interpret the first as a keyField and the second as
        #       an integer keyValue; we then do gathering only for that
        #       particular database key.

        if len(sys.argv) > 1:
                raise Exception('%s: Command-line arguments not supported' % error)
        logger.info ('Begin %s' % sys.argv[0])
        gatherer.go()
        logger.close()
        return

def logMemoryUsage():
        # Purpose: write to the log file the current memory usage for
        #       the script
        # Returns: nothing
        # Assumes: nothing
        # Modifies: writes to log file
        # Throws: nothing

        #logger.info ('RAM used : %s' % myMemory())
        return

###--- Classes ---###

class Gatherer:
        # Is: a data gatherer
        # Has: information about queries to execute, fields to retrieve, how
        #       to write data files, etc.
        # Does: queries source db for data, collates result sets, and writes
        #       out a single text file of results

        def __init__ (self,
                filenamePrefix,         # string; prefix of filename to write
                fieldOrder = None,      # list of strings; ordering of field-
                                        # ...names in output file
                cmds = None             # list of strings; queries to execute
                                        # ...against the source database to
                                        # ...extract data
                ):
                self.filenamePrefix = filenamePrefix
                self.cmds = cmds
                self.fieldOrder = fieldOrder

                self.results = []       # list of lists of query results
                self.finalResults = []  # list of collated query results
                self.finalColumns = []  # list of strings (column names) found
                                        # ...in self.finalResults

                self.nextAutoKey = None         # integer; auto-increment key
                return

        def preprocessCommands (self):
                # Purpose: to do any necessary pre-processing of the SQL
                #       queries
                # Returns: nothing
                # Assumes: nothing
                # Modifies: nothing
                # Throws: nothing

                return

        def go (self):
                # Purpose: to drive the gathering process from queries
                #       through writing the output file
                # Returns: nothing
                # Assumes: nothing
                # Modifies: queries the database, writes to the file system
                # Throws: propagates all exceptions

                self.preprocessCommands()
                logger.info ('Pre-processed queries')
                self.results = executeQueries (self.cmds)
                logger.info ('Finished queries of source %s db' % SOURCE_DB)
                self.collateResults()
                logger.info ('Built final result set (%d rows)' % \
                        len(self.finalResults))
                self.postprocessResults()
                logger.info ('Post-processed result set')

                path = OutputFile.createAndWrite (self.filenamePrefix,
                        self.fieldOrder, self.finalColumns, self.finalResults)

                print('%s %s' % (path, self.filenamePrefix))
                return

        def getTables (self):
                return [ self.filenamePrefix ]

        def collateResults (self):
                # Purpose: to do any necessary slicing and dicing of
                #       self.results to produce a final set of results to be
                #       written in self.finalResults and self.finalColumns
                # Returns: nothing
                # Assumes: nothing
                # Modifies: nothing
                # Throws: nothing

                self.finalColumns = self.results[-1][0]
                self.finalResults = self.results[-1][1]
                return

        def convertFinalResultsToList (self):
                self.finalResults = list(map (list, self.finalResults))
                logger.debug ('Converted %d tuples to lists' % \
                        len(self.finalResults))
                return

        def addColumn (self,
                columnName,
                columnValue,
                row,
                columnSet
                ):
                if columnName not in columnSet:
                        columnSet.append (columnName)
                row.append (columnValue)
                return

        def postprocessResults (self):
                # Purpose: to do any necessary post-processing of the final
                #       results before they are written out to a file
                # Returns: nothing
                # Assumes: nothing
                # Modifies: nothing
                # Throws: nothing

                return

class ChunkGatherer (Gatherer):
        # Is: a Gatherer for large data sets -- retrieves and processes data
        #       in chunks, rather than with the whole data set at once
        # Notes: Any query that must be restricted by key must contain two
        #       '%d' fields in it.  (So, for example, loading a slice of rows
        #       into a temp table might have those '%d' fields, and later
        #       queries to process the temp table rows might not.)  The first
        #       '%d' should be for the low end of the range, inclusive: 
        #       (eg- "x >= %d").  The second '%d' should be for the upper end
        #       of the range, exclusive:  (eg- "x < %d")

        def __init__ (self,
                filenamePrefix,         # string; prefix of filename to write
                fieldOrder = None,      # list of strings; ordering of field-
                                        # ...names in output file
                cmds = None             # list of strings; queries to execute
                                        # ...against the source database to
                                        # ...extract data
                ):
                Gatherer.__init__ (self, filenamePrefix, fieldOrder, cmds)
                self.baseCmds = cmds[:]
                self.chunkSize = config.CHUNK_SIZE
                return

        def setChunkSize (self, newChunkSize):
                self.chunkSize = newChunkSize
                logger.debug ('Set chunk size = %d' % self.chunkSize)
                return

        def getOutputFile (self):
                # opens and returns a suitable OutputFile object

                return OutputFile.OutputFile (self.filenamePrefix)

        def go (self):
                # We can just let key-based processing go in the traditional
                # manner.  We only need to process chunks of results if we
                # have no restriction by key.

                logger.debug ('Chunk-based update')

                # find the range of keys we need to support

                minCmd = self.getMinKeyQuery()
                maxCmd = self.getMaxKeyQuery()

                if (minCmd == None) or (maxCmd == None):
                        raise Exception('%s: Required methods not implemented' % error)

                minKey = dbAgnostic.execute(minCmd)[1][0][0]
                maxKey = dbAgnostic.execute(maxCmd)[1][0][0]

                if (minKey == None) or (maxKey == None):
                        raise Exception('%s: No data found' % error)

                logger.debug ('Found keys from %d to %d' % (minKey, maxKey))

                # create the output data file

                out = self.getOutputFile()

                # work through the data chunk by chunk

                lowKey = minKey
                while lowKey <= maxKey:
                        highKey = lowKey + self.chunkSize
                        self.results = []
                        self.finalResults = []

                        self.preprocessCommandsByChunk (lowKey, highKey)
                        self.results = executeQueries (self.cmds)
                        self.collateResults()
                        self.postprocessResults()
                        out.writeToFile (self.fieldOrder, self.finalColumns,
                                self.finalResults)
                        logger.debug ('Wrote keys %d..%d' % (
                                lowKey, highKey - 1))

                        lowKey = highKey

                # close the data file and write its path to stdout

                out.close()
                logger.debug ('Wrote %d rows to %s' % (out.getRowCount(),
                        out.getPath()) )

                print('%s %s' % (out.getPath(), self.filenamePrefix))
                return

        def getMinKeyQuery (self):
                return

        def getMaxKeyQuery (self):
                return

        def preprocessCommandsByChunk (self,
                lowKey,
                highKey
                ):
                # The commands to be updated initially live in self.baseCmds.
                # For any which include a '%d', then we fill in the lowKey and
                # highKey in their places.  Updated commands are placed in
                # self.cmds.

                self.cmds = []

                for cmd in self.baseCmds:
                        if '%d' in cmd:
                                self.cmds.append (cmd % (lowKey, highKey))
                        else:
                                self.cmds.append (cmd)
                return

class MultiFileGatherer:
        # Is: a Gatherer which handles generating multiple files rather than a
        #       single one.  This is useful for cases where we are generating
        #       unique keys which need to be used for joins to related tables.
        # Has: information about queries to execute, fields to retrieve, how
        #       to write data files, etc.
        # Does: queries source db for data, collates result sets, and writes
        #       out one or more text files of results
        # Notes: This is not a subclass of Gatherer, because it is pretty much
        #       completely re-implemented.  They share a common goal (extract
        #       data from the source database, repackage it, and write data
        #       files), but that is as far as the similarity goes.

        def __init__ (self,
                files,                  # list of (filename, field order,
                                        # ...table name) triplets for output
                cmds = None             # list of strings; queries to execute
                                        # ...against the source database to
                                        # ...extract data
                ):
                self.files = files
                self.cmds = cmds

                self.results = []       # list of lists of query results (to
                                        # ...be built from executing 'cmds')

                self.output = []        # list of (list of columns, list of
                                        # rows), with one per output file, to
                                        # be filled in by collateResults()
                self.lastWritten = None # number of last file written so far

                # Some gatherers may want to manage their own file writes
                # in the interests of saving memory.  In this case, they can
                # set this flag to skip the default method of writing at the
                # end.  Note that they also must handle writing out the
                # file paths and table names to stdout themselves.
                self.customWrites = False
                return

        def preprocessCommands (self):
                # Purpose: to do any necessary pre-processing of the SQL
                #       queries
                # Returns: nothing
                # Assumes: nothing
                # Modifies: nothing
                # Throws: nothing

                return

        def writeOneFile (self, deleteAfter = True):
                if self.lastWritten == None:
                        self.lastWritten = 0
                else:
                        self.lastWritten = 1 + self.lastWritten

                if len(self.output) < (self.lastWritten + 1):
                        raise Exception('%s: writeOneFile() failed -- data set %d not ready yet' % (error, self.lastWritten))

                filename, fieldOrder, tableName = self.files[self.lastWritten]
                columns, rows = self.output[self.lastWritten]

                path = OutputFile.createAndWrite (filename, fieldOrder,
                        columns, rows)

                print('%s %s' % (path, tableName))
                
                if deleteAfter:
                        self.output[self.lastWritten] = ([], [])
                        logger.info ('Removed data set %d after writing' % \
                                self.lastWritten)
                return

        def go (self):
                # Purpose: to drive the gathering process from queries
                #       through writing the output file
                # Returns: nothing
                # Assumes: nothing
                # Modifies: queries the database, writes to the file system;
                #       writes one line to stdout for each file written,
                #       containing the path to the file and the table into
                #       which it should be loaded
                # Throws: propagates all exceptions

                self.preprocessCommands()
                logger.info ('Pre-processed queries')
                logMemoryUsage()

                if self.cmds:
                        self.results = executeQueries (self.cmds)
                        logger.info ('Finished queries of source %s db' % \
                                SOURCE_DB)
                        logMemoryUsage()

                self.collateResults()
                logger.info ('Built %d result sets' % len(self.output))
                logMemoryUsage()

                self.postprocessResults()
                logger.info ('Post-processed result sets')
                logMemoryUsage()

                # if the gatherer is managing its own writes, then we can just
                # bail out now 
                if self.customWrites:
                        return

                if len(self.output) != len(self.files):
                        raise Exception('%s: Mismatch: %d files, %d output sets' % (error,
                                len(self.files), len(self.output) ))

                if self.lastWritten == None:
                        i = 0
                else:
                        i = 1 + self.lastWritten

                while i < len(self.files):
                        self.writeOneFile()
                        i = i + 1
                return

        def getTables (self):
                items = []
                for (filename, fieldOrder, tableName) in self.files:
                        items.append (tableName)
                return items

        def collateResults (self):
                # Purpose: to do any necessary slicing and dicing of
                #       self.results to produce a final set of results to be
                #       written in self.output
                # Returns: nothing
                # Assumes: nothing
                # Modifies: nothing
                # Throws: nothing

                raise Exception('%s: Must define collateResults() in subclass' % error)

        def postprocessResults (self):
                # Purpose: to do any necessary post-processing of the final
                #       results before they are written out to files
                # Returns: nothing
                # Assumes: nothing
                # Modifies: nothing
                # Throws: nothing

                return

class CachingMultiFileGatherer:
        # Is: a Gatherer which handles generating multiple files rather than a
        #       single one.  This is useful for cases where we are generating
        #       unique keys which need to be used for joins to related tables.
        #       This class is distinguished from a traditional MultiFileGatherer
        #       in that it uses CachingOutputFile objects to store only a
        #       portion of the results in memory, while automatically writing
        #       the others out to disk. This should be a substantial memory
        #       savings for some data sets.
        # Has: information about queries to execute, fields to retrieve, how
        #       to write data files, etc.
        # Does: queries source db for data, collates result sets, and writes
        #       out one or more text files of results
        # Notes: This is not a subclass of Gatherer, because it is pretty much
        #       completely re-implemented.  They share a common goal (extract
        #       data from the source database, repackage it, and write data
        #       files), but that is as far as the similarity goes.

        def __init__ (self,
                files,                  # list of (table name, order of fields
                                        # as submitted, order of fields to be
                                        # written out) tuples : 1 per table
                cmds = None             # list of strings; queries to execute
                                        # ...against the source database to
                                        # ...extract data
                ):
                self.baseCmds = cmds[:]

                self.results = []       # list of lists of query results (to
                                        # ...be built from executing 'cmds')

                # manages our CachingOutputFiles
                self.files = OutputFile.CachingOutputFileFactory()
                
                self.outputFiles = files

                # maps from table name (string) to the file ID in self.files
                self.tablenameToFileID = {}

                # chunking is optional, but can keep memory down for those 
                # data sets where the queries can be walked through a few keys
                # at a time.  Use the setupChunking() method to set these up
                # before calling go().

                self.chunkingOn = False
                self.minKey = None
                self.maxKey = None
                self.chunkSize = None
                self.checksums = []
                return

        def setupChunking (self,
                minKeyQuery,            # string; SQL to get minimum key
                maxKeyQuery,            # string; SQL to get maximum key
                chunkSize = 10000       # int; number of keys to do at once
                ):
                # Purpose: set up this gatherer to walk through its data in
                #       chunks, rather than doing all the results at once
                # Returns: nothing
                # Assumes: both 'minKeyQuery' and 'maxKeyQuery' return a
                #       single row with a single value
                # Modifies: four instance variables
                # Throws: propagates any exceptions if there are problems
                #       executing either 'minKeyQuery' or 'maxKeyQuery'
                #       against the database

                cols, rows = dbAgnostic.execute(minKeyQuery)
                self.minKey = max(rows[0][0], 0)                        # default to 0 if None

                cols, rows = dbAgnostic.execute(maxKeyQuery)
                self.maxKey = max(rows[0][0], 0)                        # default to 0 if None

                self.chunkSize = chunkSize
                self.chunkingOn = True

                logger.debug('Chunking on; keys %d to %d' % (self.minKey,
                        self.maxKey))
                return

        def preprocessCommands (self):
                # Purpose: to do any necessary pre-processing of the SQL
                #       queries
                # Returns: nothing
                # Assumes: nothing
                # Modifies: nothing
                # Throws: nothing

                return

        def preprocessCommandsByChunk (self, lowKey, highKey):
                # The commands to be updated initially live in self.baseCmds.
                # For any which include a '%d', then we fill in the lowKey and
                # highKey in their places.  Updated commands are placed in 
                # self.cmds.

                self.cmds = []

                for cmd in self.baseCmds:
                        if '%d' in cmd:
                                self.cmds.append (cmd % (lowKey, highKey))
                        else:
                                self.cmds.append (cmd)
                return

        def _getFileID (self, tableName):
                # returns the file ID associated with the tableName

                if tableName in self.tablenameToFileID:
                        return self.tablenameToFileID[tableName]
                raise Exception('Unknown table name: %s' % tableName)

        def setCacheSize (self, fileID, cacheSize):
                # adjust the cache size for the file with the given fileID

                self.files.setCacheSize(fileID, cacheSize)
                return

        def addRow (self, tableName, row):
                # adds the given row to the data file identified by fileID

                fileID = self._getFileID(tableName)
                self.files.addRow(fileID, row)
                return

        def addRows (self, tableName, rows):
                # adds the given list of rows to the data file identified by
                # fileID

                fileID = self._getFileID(tableName)
                self.files.addRows(fileID, rows)
                return

        def go (self, dataDir = config.DATA_DIR, actualName = False):
                # Purpose: to drive the gathering process from queries
                #       through writing the output file
                # Returns: nothing
                # Assumes: nothing
                # Modifies: queries the database, writes to the file system;
                #       writes one line to stdout for each file written,
                #       containing the path to the file and the table into
                #       which it should be loaded
                # Throws: propagates all exceptions
                
                
                # create output files
                for (tableName, inFieldOrder, outFieldOrder) in self.outputFiles:
                        fileID = self.files.createFile(tableName, inFieldOrder,
                                outFieldOrder, OutputFile.MEDIUM_CACHE, dataDir, actualName)
                        self.tablenameToFileID[tableName] = fileID

                logger.debug('Set up %d CachingOutputFiles' % len(self.outputFiles))


                self.preprocessCommands()
                logger.info ('Pre-processed queries')
                logMemoryUsage()

                if self.chunkingOn:
                        lowKey = self.minKey
                        while lowKey <= self.maxKey:
                                highKey = lowKey + self.chunkSize
                                self.results = []

                                self.preprocessCommandsByChunk(lowKey, highKey)
                                self.results = executeQueries(self.cmds)
                                self.collateResults()
                                self.postprocessResults()

                                logger.debug('Handled keys %d-%d' % (lowKey,
                                        highKey - 1))
                                lowKey = highKey 
                                logMemoryUsage()
                else:
                        # no chunking -- process all results at once

                        if self.baseCmds:
                            self.results = executeQueries (self.baseCmds)
                            logger.info ('Finished queries of source %s db' % \
                                SOURCE_DB)

                        self.collateResults()
                        logger.info ('Collated results')
                        logMemoryUsage()

                        self.postprocessResults()
                        logger.info ('Post-processed results')
                        logMemoryUsage()

                self.postscript()
                
                self.files.closeAll()
                logger.info ('Closed all output files')
                logMemoryUsage()

                self.files.reportAll()
                logger.info ('Reported file info to stdout')
                return

        def postscript (self):
                # Purpose: handles any last items on the to-do list before the output files are closed.
                #       This runs after going through the traditional processing (either in chunks or as a
                #       single large batch).
                # Returns: nothing
                # Assumes: nothing
                # Modifies: nothing
                # Throws: nothing
                
                pass
                
        def collateResults (self):
                # Purpose: to do any necessary slicing and dicing of
                #       self.results to produce a final set of results to be
                #       written in self.output
                # Returns: nothing
                # Assumes: nothing
                # Modifies: nothing
                # Throws: nothing

                raise Exception('%s: Must define collateResults() in subclass' % error)

        def postprocessResults (self):
                # Purpose: to do any necessary post-processing of the final
                #       results before they are written out to files
                # Returns: nothing
                # Assumes: nothing
                # Modifies: nothing
                # Throws: nothing

                return

class FileCacheGatherer(CachingMultiFileGatherer):
        # Is: a subclass of a CachingMultiFileGatherer that writes its output data to a file in
        #       femover's data/ directory, writes checksum files to show the data's version, and then
        #       can use those files to avoid re-fetching the data from the database for future runs
        #       (until the data change).
        
        def addChecksums(self, checksums):
                if type(checksums) == list:
                        self.checksums = self.checksums + checksums
                else:
                        self.checksums.append(checksums)
                return
        
        def go(self):
                if Checksum.allMatch(self.checksums):
                        for (tableName, inFieldOrder, outFieldOrder) in self.outputFiles:
                                print('%s/data/%s.rpt %s' % (os.environ['FEMOVER'], tableName, tableName))
                        logger.info('Checksums all match - using existing files')
                        return

                logger.info('Checksums did not match - rebuilding data files')
                
                # update the data files, then update the checksums
                CachingMultiFileGatherer.go(self, '../data', actualName = True)
                Checksum.updateAll(self.checksums)
                return
        
