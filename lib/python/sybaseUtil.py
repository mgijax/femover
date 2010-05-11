#!/usr/local/bin/python
# 
# provides utility functions for interacting with the Sybase database

import config
import db

###--- Initialization ---###

db.set_sqlLogin (config.SYBASE_USER, config.SYBASE_PASSWORD,
	config.SYBASE_SERVER, config.SYBASE_DATABASE)
db.useOneConnection (1)

###--- Globals ---###

# cache of terms already looked up
resolveCache = {}		# resolveCache[table][keyField][key] = term

###--- Functions ---###

def sql (cmds, auto='auto'):
	# Purpose: wrapper over db.sql()
	# Returns: as db.sql() would
	# Assumes: we can query Sybase
	# Modifies: nothing
	# Throws: propagates any exceptions from db.sql()

	return db.sql (cmds, auto)

def resolve (key,		# integer; key value to look up
	table = "VOC_Term",	# string; table in which to look up key
	keyField = "_Term_key",	# string; field in which to look up key
	stringField = "term"	# string; field with text correponding to key
	):
	# Purpose: to look up the string associated with 'key' in 'table'.
	#	'keyField' specifies the fieldname containing the 'key', while
	#	'stringField' identifies the field with its corresponding
	#	string value
	# Returns: string (or None if no matching key can be found)
	# Assumes: we can query Sybase
	# Modifies: adds entries to global 'resolveCache' to cache values as
	#	they are looked up
	# Throws: propagates any exceptions from db.sql()

	global resolveCache

	tableDict = None
	fieldDict = None

	if resolveCache.has_key(table):
		tableDict = resolveCache[table]
		if tableDict.has_key(keyField):
			fieldDict = tableDict[keyField]
			if fieldDict.has_key(key):
				return fieldDict[key]

	cmd = 'select %s from %s where %s = %d' % (stringField, table,
		keyField, key)
	results = db.sql (cmd, 'auto')

	if results:
		term = results[0][stringField]
	else:
		term = None

	if tableDict == None:
		tableDict = {}
		resolveCache[table] = tableDict
		
	if fieldDict == None:
		fieldDict = {}
		tableDict[keyField] = fieldDict

	fieldDict[key] = term
	return term
