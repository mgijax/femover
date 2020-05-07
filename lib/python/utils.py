# Name: utils.py
# Purpose: catchall for miscellaneous utility functions that can be shared
#	across gatherers

import logger
import dbAgnostic

# cache of chromosomes for each organism (by organism key)
#       chromosomesByOrganism[organism key][chromosome] = sequence number
chromosomesByOrganism = {}

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

def chromosomeSeqNum(chromosome, organismKey = 1):
        # returns a sequence number corresponding to the given chromosome for the given organism (default mouse)
        global chromosomesByOrganism

        if organismKey not in chromosomesByOrganism:
                cmd = '''select c.chromosome, c.sequenceNum
                        from mrk_chromosome c
                        where c._Organism_key = %d''' % organismKey
                cols, rows = dbAgnostic.execute(cmd)

                chromCol = dbAgnostic.columnNumber(cols, 'chromosome')
                seqNumCol = dbAgnostic.columnNumber(cols, 'sequenceNum')

                chromosomesByOrganism[organismKey] = {}
                for row in rows:
                        chromosomesByOrganism[organismKey][row[chromCol]] = row[seqNumCol]
                logger.debug('Cached %d chromosome rows for organism %d' % (len(rows), organismKey))

        if chromosome in chromosomesByOrganism[organismKey]:
                return chromosomesByOrganism[organismKey][chromosome]
        return 99999

def intSortKey(i):
        # return an integer sort key with None values coming first
        if i == None:
                return -999999
        return i

def stringSortKey(s, noneFirst=False):
        # return string sort key with None values coming last
        if type(s) == str:
                return s
        if noneFirst:
                return ' '
        return 'zzzzzzzzzz'

def floatSortKey(f, noneFirst=False):
        # return float sort key with None values coming last
        if type(f) == float:
                return f
        if type(f) == int:
                return float(f)
        if noneFirst:
                return -99999999
        return 99999999999999
