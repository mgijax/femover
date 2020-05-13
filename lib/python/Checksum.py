# Name: Checksum.py
# Purpose: provides a mechanism for computing a checksum, writing it to a file, then comparing 
#   a new checksum to the value in the file for future runs (works with Gatherer.FileCacheGatherer)

import os
import dbAgnostic
import logger

###--- Globals ---###

maxListSize = 1000

###--- Functions ---###

def allMatch (
    checksums       # list of Checksum objects
    ):
    # Process the list of checksums.  If all match previous value, return True.
    # Otherwise return False (indicating data needs to be regenerated).
    
        for checksum in checksums:
                if not checksum.matches():
                        return False
        return True

def updateAll (
    checksums       # list of Checksum objects
    ):
    # Process the list of checksums and write an updated value for each to the
    # file system.
    
        for checksum in checksums:
                checksum.update()
        return
 
def singleCount (
    sqlCmd          # string; SQL command that returns a single count
    ):
    # execute the given 'sqlCmd' that returns a single integer value, and
    # return that value.  Useful just for checking a count.
    
    logger.debug('Computing checksum: %s' % sqlCmd)
    cols, rows = dbAgnostic.execute(sqlCmd)
    if rows:
        return rows[0][0]       # return first item from first row
    return 0                    # no rows, so go with zero

def hashResults (
    sqlCmd          # string; SQL command that returns one or more rows of results
    ):
    # execute the given 'sqlCmd' that returns one or more rows of results, hash each row,
    # hash the row hashes together, and return a single integer value.  Useful for 
    # checking to see if a series of results has changed at all.
    
    logger.debug('Computing checksum: %s' % sqlCmd)
    cols, rows = dbAgnostic.execute(sqlCmd)
    if rows:
        hashes = []
        for row in rows:
            hashes.append(hash(row))
            if len(hashes) >= maxListSize:
                hashes = [ hash(tuple(hashes)) ]
        return hash(tuple(hashes))
    return 0                    # no rows, so go with zero

###--- Classes ---###

class Checksum:
    def __init__ (self,
        prefix,     # string; prefix of filename
        dataDir,    # string; path to where file should be written
        newValue    # integer; new value for checksum (call function when instantiating object)
        ):
        self.filename = prefix + '.checksum'
        self.dataDir = dataDir
        self.newValue = newValue
        return
    
    def matches(self):
        # return True if the new value of the checksum matches the previous, False if not
        if self.newValue == self._loadValue():
            logger.info('%s matches' % self.filename)
            return True

        logger.info('%s does not match' % self.filename)
        return False

    def update(self):
        # update the file with the new value of the checksum

        path = self._getPath()
        fp = open(path, 'w')
        outputLine = '%s' % self.newValue
        fp.write(outputLine.encode())
        fp.close()
        logger.info('Wrote %d for %s' % (self.newValue, self.filename))
        return

    def _getPath(self):
        # get the full path to the data file with the checksum value
        return os.path.join(self.dataDir, self.filename)
        
    def _loadValue(self):
        # return the integer hash value for this checksum, as currently stored in the
        # corresponding file
        
        path = self._getPath()
        if os.path.exists(path):
            fp = open(path, 'r')
            line = fp.readline()
            fp.close()
            return int(line.strip())
        return None
