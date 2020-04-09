# Module: TagConverter.py
# Purpose: to provide for conversion of any special MGI markup tags that we
#	want to process in the mover (as opposed to in the web products)

import dbAgnostic
import logger
import types
import re

###--- Globals ---###

# allele ID -> current allele symbol
ALLELES = {}

# regex to match the \AlleleSymbol(id|0) tag and pull out the ID in group 1
ALLELE_SYMBOL = re.compile ('\\\\AlleleSymbol\(([^|)]+)\|[01]\)')

###--- Private Functions ---###

def _initialize():
	global ALLELES

	ALLELES = {}

	# at the moment, we only support MGI IDs for alleles
	query = '''select acc.accID, a.symbol
		from acc_accession acc, all_allele a
		where acc._Object_key = a._Allele_key
			and acc._MGIType_key = 11
			and acc._LogicalDB_key = 1'''

	(cols, rows) = dbAgnostic.execute (query)

	idCol = dbAgnostic.columnNumber (cols, 'accID')
	symbolCol = dbAgnostic.columnNumber (cols, 'symbol')

	for row in rows:
		ALLELES[row[idCol]] = row[symbolCol]
	logger.debug ('Found %d alleles' % len(ALLELES))
	return

def _superscript(s):
	# handles multiple superscript indicators (angle brackets) in 's',
	# assuming that they are not nested.
	#	eg: 'A<b><c>' is okay, but 'A<b<c>>' is not

	inSuperScript = False
	t = ''

	for c in s:
		if (c == '<'):
			if not inSuperScript:
				inSuperScript = True
				c = '<sup>'
			else:
				# escape a redundant open bracket
				c = '&lt;'

		elif (c == '>'):
			if inSuperScript:
				inSuperScript = False
				c = '</sup>'
			else:
				# escape a redundant close bracket
				c = '&gt;'
		t = t + c
	
	# just to be friendly...  if we have an unclosed open bracket, then
	# add a close bracket so the rest of the page isn't superscripted

	if inSuperScript:
		t = t + '</sup>'
	return t

def _convertString (s,superscript=True):
	match = ALLELE_SYMBOL.search(s)
	lastIndex = 0
	t = ''

	while match:
		start, stop = match.regs[0]
		alleleID = match.group(1)

		if ALLELES.has_key(alleleID):
			if superscript:
				symbol = _superscript(ALLELES[alleleID])
			else:
				symbol = ALLELES[alleleID]
		else:
			symbol = alleleID

		t = t + s[lastIndex:start] + symbol
		lastIndex = stop

		match = ALLELE_SYMBOL.search(s, stop)

	t = t + s[lastIndex:]
	return t

###--- Functions ---###

def convert (s,superscript=True):
	# convert any tags contained in 's'
	global ALLELES

	if ALLELES == {}:
		_initialize()

	if type(s) == types.StringType:
		return _convertString(s,superscript)

	if type(s) == types.ListType:
		t = []
		for item in s:
			t.append (convert(item))
		return t
	return s
