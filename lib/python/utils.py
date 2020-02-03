# Name: utils.py
# Purpose: catchall for miscellaneous utility functions that can be shared
#	across gatherers

import logger
import dbAgnostic

def cleanupOrganism (organism):
	# translate raw 'organism' value from the database to a version
	# preferred for front-end display purposes

	if organism == 'mouse, laboratory':
		return 'mouse'
	if organism == 'dog, domestic':
		return 'dog'
	return organism

def fillDictionary(dataType, cmd, dict, keyField, valueField, cleanupFn = None):
	# populates the global dictionary specified in 'dict' with values returned from 'cmd'
	
	logger.debug('Caching ' + dataType)
	cols, rows = dbAgnostic.execute(cmd)
	logger.debug(' - returned %d rows from db' % len(rows))

	keyCol = dbAgnostic.columnNumber(cols, keyField)
	valueCol = dbAgnostic.columnNumber(cols, valueField)
	
	for row in rows:
		if cleanupFn:
			dict[row[keyCol]] = cleanupFn(row[valueCol])
		else:
			dict[row[keyCol]] = row[valueCol]
	
	logger.debug(' - cached %d %s' % (len(dict), dataType))
	return
