# Module: GXDUtils.py
# Purpose: to provide handy utility functions for dealing with GXD data

import dbAgnostic
import logger

###--- functions dealing with ages ---###

# list of (old, new) pairs for use in seek-and-replace loops in abbreviate()
TIMES = [ (' day ',''), ('week ','w '), ('month ','m '), ('year ','y ') ]

# list of (old, new) pairs for use in seek-and-replace loops in abbreviate()
QUALIFIERS = [ ('embryonic', 'E'), ('postnatal', 'P') ]

def abbreviateAge (
        s               # string; specimen's age from gxd_expression.age
        ):
        # Purpose: convert 's' to a more condensed format for display on a
        #       query results page
        # Returns: string; with substitutions made as given in 'TIMES' and
        #       'QUALIFIERS' above.
        # Assumes: 's' contains at most one value from 'TIMES' and one value
        #       from 'QUALIFIERS'.  This is for efficiency, so we don't have
        #       to check every one for every invocation.
        # Effects: nothing
        # Throws: nothing

        # we have two different lists of (old, new) strings to check...
        for items in [ TIMES, QUALIFIERS ]:
                for (old, new) in items:

                        # if we do not find 'old' in 's', then we just go back
                        # up to continue the inner loop.

                        pos = s.find(old)
                        if pos == -1:
                                continue

                        # otherwise, we replace 'old' with 'new' and break out
                        # of the inner loop to go back to the outer one.

                        s = s.replace(old, new)
                        break
        return s

###--- functions dealing with EMAPA/EMAPS ---###

EMAPS_TO_EMAPA = None	# dictionary mapping EMAPS key to EMAPA key
EMAPA_STAGE_RANGE = None	# dictionary mapping EMAPA key -> range string
EMAPA_TERM = None		# dictionary mapping EMAPA key -> term
EMAPA_START_STAGE = None	# dictionary mapping EMAPA key -> start stage

def getEmapaKey (emapsKey):
	# get the EMAPA key corresponding to the given EMAPS key

	global EMAPS_TO_EMAPA

	if EMAPS_TO_EMAPA == None:
		query = '''select _emapa_term_key, _term_key
			from voc_term_emaps'''

		(cols, rows) = dbAgnostic.execute(query)

		EMAPS_TO_EMAPA = {}

		emapsCol = dbAgnostic.columnNumber (cols, '_term_key')
		emapaCol = dbAgnostic.columnNumber (cols, '_emapa_term_key')

		for row in rows:
			EMAPS_TO_EMAPA[row[emapsCol]] = row[emapaCol]

		logger.debug('Mapped %d EMAPS terms to EMAPA' % \
			len(EMAPS_TO_EMAPA))

	if EMAPS_TO_EMAPA.has_key(emapsKey):
		return EMAPS_TO_EMAPA[emapsKey]
	return None

def getEmapaStageRange (emapaKey):
	# get the stage range (eg- "TS1-5") for the given EMAPA key

	global EMAPA_STAGE_RANGE

	if EMAPA_STAGE_RANGE == None:
		query = '''select _term_key, startStage, endStage
			from voc_term_emapa'''

		(cols, rows) = dbAgnostic.execute(query)

		EMAPA_STAGE_RANGE = {}

		emapaCol = dbAgnostic.columnNumber (cols, '_term_key')
		startCol = dbAgnostic.columnNumber (cols, 'startStage')
		endCol = dbAgnostic.columnNumber (cols, 'endStage')

		for row in rows:
			startStage = row[startCol]
			endStage = row[endCol]

			if startStage != endStage:
				s = 'TS%s-%s' % (startStage, endStage)
			else:
				s = 'TS%s' % startStage

			EMAPA_STAGE_RANGE[row[emapaCol]] = s

		logger.debug('Got stage ranges for %d EMAPA terms' % \
			len(EMAPA_STAGE_RANGE))

	if EMAPA_STAGE_RANGE.has_key(emapaKey):
		return EMAPA_STAGE_RANGE[emapaKey]
	return None

def getEmapaTerm (emapaKey):
	# get the structure term for the given EMAPA key

	global EMAPA_TERM

	if EMAPA_TERM == None:
		query = '''select t._term_key, t.term
			from voc_term t, voc_vocab v
			where t._vocab_key = v._vocab_key
				and v.name = 'EMAPA' '''

		(cols, rows) = dbAgnostic.execute(query)

		EMAPA_TERM = {}

		emapaCol = dbAgnostic.columnNumber (cols, '_term_key')
		termCol = dbAgnostic.columnNumber (cols, 'term')

		for row in rows:
			EMAPA_TERM[row[emapaCol]] = row[termCol]

		logger.debug('Got structures for %d EMAPA terms' % \
			len(EMAPA_TERM))

	if EMAPA_TERM.has_key(emapaKey):
		return EMAPA_TERM[emapaKey]
	return None

def getEmapaStartStage (emapaKey):
	# get the start stage (as an integer) for the given EMAPA key

	global EMAPA_START_STAGE

	if EMAPA_START_STAGE == None:
		query = 'select _term_key, startStage from voc_term_emapa'

		(cols, rows) = dbAgnostic.execute(query)

		EMAPA_STAGE_RANGE = {}

		emapaCol = dbAgnostic.columnNumber (cols, '_term_key')
		startCol = dbAgnostic.columnNumber (cols, 'startStage')

		EMAPA_START_STAGE = {}

		for row in rows:
			EMAPA_START_STAGE[row[emapaCol]] = \
				int(row[startCol])

		logger.debug('Got start stages for %d EMAPA terms' % \
			len(EMAPA_START_STAGE))

	if EMAPA_START_STAGE.has_key(emapaKey):
		return EMAPA_START_STAGE[emapaKey]
	return None

