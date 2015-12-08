# Module: AnnotationKeyGenerator.py
# Purpose: to provide an easy mechanism for generating new annotation keys
#	for the front-end database's annotation* tables (and for looking up
#	keys we have already generated).  This includes encapsulating the
#	differing rules among different types of annotations so we don't need
#	to replicate them or complicate other code with them.

###--- Globals ---###

# standard exception raised in this module
error = 'AnnotationKeyGenerator.error'

# error message when a keyword parameter is missing
MISSING_KEYWORD = 'Missing keyword (%s) in call to getKey()'

# maximum annotation key assigned so far
MAX_ANNOT_KEY = 0

# mgd _Annot_key --> (annotation type, object key, term key, qualifier)
ANNOT_CACHE = {}

###--- Functions ---###

def _nextAnnotKey():
	# (private to this module) get the next available annotation key

	global MAX_ANNOT_KEY

	MAX_ANNOT_KEY = MAX_ANNOT_KEY + 1
	return MAX_ANNOT_KEY

def cacheAnnotationAttributes (mgdAnnotKey, annotType, objectKey, termKey,
		qualifier):
	# cache the attributes for this annotation

	global ANNOT_CACHE

	ANNOT_CACHE[mgdAnnotKey] = (annotType, objectKey, termKey, qualifier)
	return

def getAnnotationAttributes (mgdAnnotKey):
	# retrieve the basic attributes for this annotation as a tuple:
	#	(annotation type, object key, term key, qualifier)

	if ANNOT_CACHE.has_key(mgdAnnotKey):
		return ANNOT_CACHE[mgdAnnotKey]
	return None, None, None, None

###--- Classes ---###

class KeyGenerator:
	def __init__ (self):
		# constructor

		# canonical data -> annotation key
		self.lookup = {}

		# mapping of extra function arguments from getKey() -- used to
		# make subclassing easier
		self.callData = {}

		# list of required keyword arguments
		self.setRequiredKeywords()
		return

	def setRequiredKeywords (self):
		self.requiredKeywords = []
		return

	def getKey (self, mgdAnnotKey, **kw):
		# get the new annotation key for the given mgd annotation key
		# and any extra associated keyword arguments

		# verify that all required keyword arguments were submitted

		for keyword in self.requiredKeywords:
			if not kw.has_key(keyword):
				raise error, MISSING_KEYWORD % keyword 

		# we have all needed arguments, so get a tuple for the set of
		# data that canonically defines an annotation to be unique for
		# this type of annotation

		self.callData = kw
		cd = self.canonicalData(mgdAnnotKey)

		# look up the new annotation key for this annotation (assign
		# one if we haven't seen this annotation before)

		if not self.lookup.has_key(cd):
			self.lookup[cd] = _nextAnnotKey()

		return self.lookup[cd]

	def canonicalData (self, mgdAnnotKey):
		# get a tuple representing the set of data that uniquely
		# identifies this as a unique annotation (including any
		# keyword arguments passed along through self.callData)

		(annotType, objectKey, termKey, qualifier) = \
			getAnnotationAttributes(mgdAnnotKey)

		# grab the set of mandatory data

		data = [mgdAnnotKey, annotType, objectKey, termKey, qualifier]

		# append any extra required arguments

		extraArgs = self.callData.keys()
		extraArgs.sort()

		for key in extraArgs:
			data.append (self.callData[key])

		return tuple(data)
	
# TODO (kstone): These classes should be named after what they are used
#	for, not what values they receive
#	E.g. GO annotations, MP annotations, PRO annotations, etc
#

class EvidenceInferredKeyGenerator (KeyGenerator):
	def setRequiredKeywords (self):
		self.requiredKeywords = [ 'evidenceTermKey', 'inferredFrom' ]
		return

class TermMarkerKeyGenerator (KeyGenerator):
	def setRequiredKeywords (self):
		self.requiredKeywords = [ 'termID', 'markerKey' ]
		return

class TermMarkerSpecialKeyGenerator (KeyGenerator):
	def setRequiredKeywords (self):
		self.requiredKeywords = [ 'termID',
			'markerKey', 'specialType' ]
		return

class AccessionTermMarkerSpecialKeyGenerator (KeyGenerator):
	def setRequiredKeywords (self):
		self.requiredKeywords = [ 'accessionKey',
			'termID', 'markerKey', 'specialType' ]
		return

class SpecialKeyGenerator (KeyGenerator):
	def setRequiredKeywords (self):
		self.requiredKeywords = [ 'specialType' ]
		return

