#!/usr/local/bin/python

# Name: postgresTextCleaner.py
# Purpose: 
#	1. to escape any embedded newlines in fields in the input stream
#	(but do not escape any which should be delimiting the end of the
#	record)
#	2. to escape any embedded tabs in fields in the input stream
#	3. to remove any unacceptable characters (lots of different escape and
#	control sequences appear in some fields for some reason...)
#	4. to strip the record-terminating #=# at the end of each record
# Usage: as a filter, with input piped into stdin and output written to stdout
 
import sys 
import string
import re

# in empirical testing, the fastest character check is to use the 'in'
# operator with a string of acceptable values.  (a dictionary is roughly
# 2x as slow, while a list is about 4.5x as slow)

# which characters do we want to allow in the various fields; anything not in
# this string is omitted from the output (except tab and newline, which are
# escaped)
acceptableCharacters = string.digits + string.letters + \
	'''-&=#,:. ?+<>()'"/[]*;'''

# If this line does not end with our '#=#\n' delimiter, then the next line is
# a continuation of it and should be appended.

fp = sys.stdin
line = fp.readline()

partialLine = ''	# beginning of the current record, from prior line(s)

while line:
	# As an optimization, assume that all are acceptable characters.  If
	# this is true, then just use 'line' as-is.  If we discover an
	# unacceptable character, then we need to work harder...  this will
	# avoid many string concatenations.

#	okay = True
#	i = 0
#	lineLength = len(line)
#	while i < lineLength:
#		if line[i] not in acceptableCharacters:
#			okay = False
#			break
#		i = i + 1
#
#	if okay:
#		cleanLine = line[:-1]		# strip trailing newline
#	else:
#		# it's okay up to the first unacceptable character, so just
#		# copy that part.  then walk character to character to ensure
#		# that we only get acceptable ones.  Note that this strips the
#		# trailing newline.
#
#		cleanLine = line[:i]
#		for c in line[i:]:
#			if c in acceptableCharacters:
#				cleanLine = cleanLine + c
#			elif c == '\t':
#				cleanLine = cleanLine + '\\\t'

	last = 0
	i = 0
	lineLength = len(line) - 1	# (skip final newline)
	cleanLine = ''
	while i < lineLength:
		if line[i] not in acceptableCharacters:
			if line[i] == '\t':
				cleanLine = cleanLine + line[last:i] + '\\\t'
			else:
				cleanLine = cleanLine + line[last:i]
			last = i + 1
		i = i + 1
	
	cleanLine = cleanLine + line[last:lineLength]

	# if this input line finishes a record, then we need to see if it is
	# the conclusion of a multi-line record or if it is a one-line record
	# and write it out correctly in either case

	if cleanLine[-3:] == '#=#':
		if partialLine:
			print partialLine + cleanLine[:-3]
			partialLine = ''
		else:
			print cleanLine[:-3]
	else:
		# otherwise, we need to keep this line as part of a record
		# which will be terminated by a subsequent line.

		# add in an escaped version of the embedded newline when
		# appending to a partial line
		partialLine = partialLine + cleanLine + '\\\n'

	line = fp.readline()

# if we ended with a partial line, write it out (should not happen, as BCP
# should properly terminate all records with #=#)
if partialLine:
	print partialLine
