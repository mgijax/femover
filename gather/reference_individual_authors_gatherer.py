#!/usr/local/bin/python
# 
# gathers data for the 'referenceIndividualAuthors' table in the front-end
# database

import Gatherer
import re

###--- Functions ---###

def cleanAuthorString (x):
	# Purpose: to clean extraneous items out of author string 'x' and
	#	leave only the authors we want to store individually,
	#	separated by semi-colons
	# Returns: string, modified from 'x'
	# Assumes: x is a string and is non-None
	# Modifies: nothing
	# Throws: nothing

	# revisions to x:
	#	1. remove "URL:" and its following spaces
	#	2. remove http: up to any comma or space

	x = re.sub ('URL: +', '', x)
	x = re.sub ('[Hh][Tt][Tt][Pp]:[^,; ]+', '', x)
	return x

def formatAuthor (x):
	# Purpose: to standardize the capitalization of author 'x', so that
	#	any instances of 'x' with varying capitalization will be
	#	represented by a single instance in the authors pick-list
	# Returns: string, modified from 'x'
	# Assumes: 'x' is a string and is non-None
	# Modifies: nothing
	# Throws: nothing
	# Notes: The following rules apply for our standard capitalization:
	#	1. 'x' will be split into multiple tokens on whitespace
	#	2. the first letter of each token will be uppercase and the
	#	   remaining letters will be lowercase, except:
	#		a. if the last token has < 4 characters, it is assumed
	#		   to be initials and will be all uppercase (unless
	#		   2b applies)
	#		b. if the last token is 'jr' or begins with a digit,
	#		   then the last token gets only an initial capital
	#		   and the next to last token is assumed to be
	#		   initials (if < 4 characters) and will be all
	#		   uppercase

	if not x:
		return x

	tokens = map (lambda y : y.lower().capitalize(), x.split() )

	# if a token is a hyphenated name, then we also need to capitalize the
	# first letter of the second name.  if multiple hyphens, just
	# capitalize the final name, not all the intervening ones (which is
	# correct for most cases).

	t = []
	for token in tokens:
		if token.find('-') >= 0:
			items = token.split('-')
			items[-1] = items[-1].capitalize()
			token = '-'.join(items)

		t.append (token)

	tokens = t 

	# if the last token has < 4 charcters and is 'Jr' or begins with a
	# digit (like '3d', '4th'), then we need to look at the next to last
	# token for initials

	lastToken = len(tokens) - 1

	if len(tokens[lastToken]) < 4:
		if (tokens[lastToken] == 'Jr') or \
			(tokens[lastToken] == 'Jr.') or \
			('0' <= tokens[lastToken][0] <= '9'):
				lastToken = lastToken - 1
	
		# look to see if we need to uppercase an entire token as
		# initials

		if len(tokens[lastToken]) < 4:
			tokens[lastToken] = tokens[lastToken].upper()

	return ' '.join(tokens)

###--- Classes ---###

class ReferenceIndividualAuthorsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the referenceIndividualAuthors table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for authors per
	#	reference, collates results, writes tab-delimited text file
	
	def collateResults (self):
		# Notes: We need to join the authors and authors2 fields into
		#	a single string, then split it up into individual
		#	authors, each of which will have their own record.

		self.finalColumns = self.fieldOrder[1:]
		self.finalResults = []

		rawColumns = self.results[-1][0]

		refsKeyCol = Gatherer.columnNumber (rawColumns, '_Refs_key')
		authorsCol = Gatherer.columnNumber (rawColumns, 'authors')
		authors2Col = Gatherer.columnNumber (rawColumns, 'authors2')

		for r in self.results[-1][1]:
			longAuthors = None
			if r[authorsCol] != None:
				longAuthors = r[authorsCol]
				if r[authors2Col] != None:
					longAuthors = longAuthors + \
						r[authors2Col]

			if longAuthors:
				refsKey = r[refsKeyCol]

				cleanAuthors = cleanAuthorString (longAuthors)
				authorList = cleanAuthors.split (';')
				authors = map (lambda x : x.strip(),
					authorList)

				seqNum = 1
				lenAuthors = len(authors)
				for author in authors:
					if seqNum == lenAuthors:
						isLast = 1
					else:
						isLast = 0

					row = [ refsKey,
						formatAuthor(author),
						seqNum,
						isLast
						]
					seqNum = seqNum + 1

					self.finalResults.append (row)
		return

###--- globals ---###

cmds = [
	'select _Refs_key, authors, authors2 from bib_refs',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'referenceKey', 'author', 'sequenceNum', 'isLast',
	]

# prefix for the filename of the output file
filenamePrefix = 'reference_individual_authors'

# global instance of a ReferenceIndividualAuthorsGatherer
gatherer = ReferenceIndividualAuthorsGatherer (filenamePrefix, fieldOrder,
	cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
