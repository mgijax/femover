# Module: KeyGenerator.py
# Purpose: to provide an easy means for generating unique keys for tables,
#	where those keys will need to be accessible to other tables

import gc

class KeyGenerator:
	def __init__ (self, tableName = None):
		# constructor

		self.tableName = tableName  # name of the table
		self.dataToKey = {}	    # maps data tuples to their keys
		self.maxKey = 0		    # max key assigned so far
		return

	def nextKey (self):
		# Assign the next available key and return it.

		self.maxKey = self.maxKey + 1
		return self.maxKey

	def getKey (self, dataTuple):
		# Look up the key assigned to 'dataTuple' and return it.  If
		# this is the first time we've seen 'dataTuple', then assign
		# a new key and return it.

		if not self.dataToKey.has_key(dataTuple):
			self.dataToKey[dataTuple] = self.nextKey()
		return self.dataToKey[dataTuple]

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
