# Module: KeyGenerator.py
# Purpose: to provide an easy means for generating unique keys for tables,
#       where those keys will need to be accessible to other tables

import gc

class KeyGenerator:
        def __init__ (self, tableName = None, bidirectional = False):
                # constructor

                self.tableName = tableName  # name of the table
                self.dataToKey = {}         # maps data tuples to their keys
                self.maxKey = 0             # max key assigned so far
                self.keyToData = {}             # maps from key back to its data
                
                # do we want to be able to map from a key back to its data?
                self.bidirectional = bidirectional
                return

        def nextKey (self):
                # Assign the next available key and return it.

                self.maxKey = self.maxKey + 1
                return self.maxKey

        def getKey (self, dataTuple):
                # Look up the key assigned to 'dataTuple' and return it.  If
                # this is the first time we've seen 'dataTuple', then assign
                # a new key and return it.

                if dataTuple not in self.dataToKey:
                        self.dataToKey[dataTuple] = self.nextKey()
                        if self.bidirectional:
                                self.keyToData[self.dataToKey[dataTuple]] = dataTuple

                return self.dataToKey[dataTuple]

        def getDataValue (self, key):
                # retrieve the data value that is associated with the 'key', or None if 
                # either the 'key' is invalid or if this object was not instantiated with
                # the 'bidirectional' flag set
                
                if key in self.keyToData:
                        return self.keyToData[key]
                return None
        
        def getDataValues (self):
                # get the data values that currently have assigned keys

                return list(self.dataToKey.keys())
        
        def __len__ (self):
                # number of values assigned
                
                return len(self.dataToKey)
        
        def getTableName (self):
                # Retrieve the table name for this KeyGenerator.

                return self.tableName

        def forget (self):
                # In some applications, you may want to try to conserve memory
                # as much as possible.  If you've passed through data that you
                # know you won't need to look up the keys again, use this
                # method to tell the KeyGenerator to forget that data.  It
                # will not re-use the assigned keys, but will keep counting
                # upward to assign new keys from here onward.  It will simply
                # forget what all the existing keys map to.

                del self.dataToKey
                self.dataToKey = {}
                gc.collect()
                return
