# Module: ListSorter.py
# Purpose: to provide a flexible sorting mechanism for a list of lists,
#	where we want to sort the sublists by multiple fields of various types

import types
import logger
import symbolsort

###--- Globals ---###

# exception raised by this module
error = 'ListSorter.error'

# valid field types:

NUMERIC = 0		# sort numerically (integers and/or floats)
ASCII = 1		# sort alphabetically, but case sensitive
ALPHA = 2		# sort alphabetically, case insensitive
SMART_ALPHA = 3		# sort alphabetically, case insensitive, but also sort
			#   embedded numbers intelligently; (eg- 'abc12' needs
			#   to come after 'abc2')

SORTBY = []	# list of (field ID, field type) tuples to specify sorting;
		#    If sorting lists, then ID should be a column index.  If
		#    sorting dictionaries, then ID should be a dictionary key.

###--- Private Functions ---###

def _listValue (myList, index):
	# return the value at the given 'index' from 'myList', defaulting to
	# None if 'myList' is too short

	if index < len(myList):
		return myList[index]
	return None

def _dictValue (myDict, key):
	# return the value for the given 'key' from 'myDict', defaulting to
	# None if 'myDict' does not have the given 'key'

	if myDict.has_key(key):
		return myDict[key]
	return None

def _convert (value, fieldType):
	# convert the given 'value' for any special handling for the given
	# 'fieldType'

	if value == None:
		return value

	# treat all numeric fields as floats, as this is just for sorting
	if fieldType == NUMERIC:
		try:
			return float(value)
		except:
			return None

	# for ASCII sorting, we also convert integers to strings
	elif fieldType == ASCII:
		return str(value)

	# for true alphabetic sorting, we convert everything to lowercase
	elif fieldType == ALPHA:
		return str(value).lower()

	# for smart-alpha sorting, we can leave value as-is, as it will get
	# case converted and broken up later on
	return value

###--- Public Functions ---###

def setSortBy (sortBy):
	# set the module up for sorting according to the given criteria
	# ('sortBy' is a list of tuples, as described above for global SORTBY)

	global SORTBY

	for (fieldID, fieldType) in sortBy:
		if fieldType not in [ NUMERIC, ASCII, ALPHA, SMART_ALPHA ]:
			raise error, 'Invalid field type: %s' % fieldType

	SORTBY = sortBy
	return

def compare (a, b):
	# comparison function for call by the standard list.sort() method,
	# comparing 'a' and 'b' according to the criteria most recently set
	# using setSortBy().

	# by default, just do a standard Python comparison

	if len(SORTBY) == 0:
		return cmp(a, b)

	# We need to ensure that 'a' and 'b' are the same type of object, and
	# they are a type we know how to deal with.

	typeA = type(a)
	typeB = type(b)

	if typeA != typeB:
		raise error, 'type(a) != type(b) in compare(a,b)'

	if typeA not in [ types.ListType, types.DictionaryType ]:
		raise error, 'type(a) not in [ List, Dictionary ]'

	for (fieldID, fieldType) in SORTBY:
		if typeA == types.DictionaryType:
			valueA = _dictValue(a, fieldID)
			valueB = _dictValue(b, fieldID)
		else:
			valueA = _listValue(a, fieldID)
			valueB = _listValue(b, fieldID)

		valueA = _convert(valueA, fieldType)
		valueB = _convert(valueB, fieldType)
	
		if fieldType == SMART_ALPHA:
			cmpAB = symbolsort.nomenCompare(valueA, valueB)
		else:
			cmpAB = cmp(valueA, valueB)

		if cmpAB != 0:
			return cmpAB

	# We never found a non-matching field, so fall back on Python default
	# comparison.

	return cmp(a, b)
