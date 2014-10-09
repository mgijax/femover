# Module: Lookup.py
# Purpose: to provide a class for looking up values for	various fields in
#	a database table when given a value for one field

import dbAgnostic
import logger

###--- Classes ---###

class Lookup:
	# Is: a mapping from keys to values, where each key has its value
	#	pulled from the database and cached in memory
	# Has: a table name, a field name to search in, a field name to return,
	#	a flag indicating it's a string search, and a flag to indicate
	#	the search should be case-insensitive
	# Does: searches in a database table to find a record with a key value
	#	in one field and returns the value of another field in that
	#	record; caches those key/value pairs for efficiency of future
	#	lookups in the same process

	def __init__ (self,
		tableName,
		searchFieldName,
		returnFieldName,
		stringSearch = False,
		caseInsensitive = False
		):
		self.tableName = tableName
		self.searchFieldName = searchFieldName
		self.returnFieldName = returnFieldName
		self.stringSearch = stringSearch
		self.caseInsensitive = caseInsensitive
		self.cache = {}
		return

	def get (self, searchTerm):
		if self.cache.has_key(searchTerm):
			return self.cache[searchTerm]

		if not self.stringSearch:
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
