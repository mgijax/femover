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
 
import os
import sys 
import string
import re
import time
import subprocess

# in empirical testing, the fastest character check is to use the 'in'
# operator with a string of acceptable values.  (a dictionary is roughly
# 2x as slow, while a list is about 4.5x as slow)

# which characters do we want to allow in the various fields; anything not in
# this string is omitted from the output (except tab and newline, which are
# escaped)
ACCEPTABLE_CHARACTERS = string.digits + string.letters + \
	'''-&=#,:. ?+<>()'"/[]*;\\'''

# number of subprocesses to use
SUBPROC_COUNT = 4

# debugging on or off?
DEBUG = False

OVER = 'Over'
QUIT = 'Quit'

START_TIME = time.time()

def debug (s):
	if not DEBUG:
		return
	sys.stderr.write ('%6.3f : %s\n' % (time.time() - START_TIME, s) )
	return

def childDebug (s):
	if not DEBUG:
		return
	return

def cleanLines(lines):
	# If this line does not end with our '#=#\n' delimiter, then the next
	# line is a continuation of it and should be appended.

	# beginning of the current record, from prior line(s)
	partialLine = ''

	output = []

	for line in lines:
		# As an optimization, assume that all are acceptable
		# characters.  If this is true, then just use 'line' as-is.
		# If we discover an unacceptable character, then we need to
		# work harder...  this will avoid many string concatenations.

		last = 0
		i = 0
		lineLength = len(line) - 1	# (skip final newline)
		cleanLine = ''
		while i < lineLength:
			if line[i] not in ACCEPTABLE_CHARACTERS:
				if line[i] == '\t':
					cleanLine = cleanLine + line[last:i] \
						+ '\\\t'
				else:
					cleanLine = cleanLine + line[last:i]
				last = i + 1
			i = i + 1
	
		cleanLine = cleanLine + line[last:lineLength]

		# if this input line finishes a record, then we need to see if
		# it is the conclusion of a multi-line record or if it is a
		# one-line record and write it out correctly in either case

		if cleanLine[-3:] == '#=#':
			if partialLine:
				output.append (partialLine + cleanLine[:-3])
				if cleanLine[:-3] == '':
					output.append ('\n')
				partialLine = ''
			else:
				output.append (cleanLine[:-3])
		else:
			# otherwise, we need to keep this line as part of a
			# record which will be terminated by a subsequent
			# line.  

			# add in an escaped version of the embedded newline
			# when appending to a partial line
			partialLine = partialLine + cleanLine + '\\\n'

	# if we ended with a partial line, write it out (should not happen, as
	# BCP should properly terminate all records with #=#)
	if partialLine:
		output.append (partialLine)

	return output

def childMain():
	sys.stdout.write (OVER + '\n')
	sys.stdout.flush()
	childDebug ('Child sent initial OVER signal')

	while True:
		lines = [] 
		line = sys.stdin.readline()

		while not line.startswith(OVER):
			if line.startswith(QUIT):
				childDebug ('Subprocess received QUIT signal')
				return
			lines.append (line)
			line = sys.stdin.readline()

		childDebug ('Subprocess received %d lines' % len(lines))
		lines = cleanLines (lines)
		for line in lines:
			sys.stdout.write (line)
			if line[-1] != '\n':
				sys.stdout.write ('\n')
		sys.stdout.write (OVER + '\n')
		childDebug ('Child wrote lines and OVER signal')
		sys.stdout.flush()
	return

def subprocIO (buckets, processes):
	# gather any new results, then send out new buckets of input

	i = 0
	for process in processes:
		# each process will have a series of output lines, ending with
		# a special OVER flag.  echo those to stdout.

		debug ('Waiting for output from subprocess %d' % process.pid)

		ct = 0
		line = process.stdout.readline()

		while not line.startswith (OVER):
			sys.stdout.write (line)
			ct = ct + 1
			line = process.stdout.readline()

		sys.stdout.flush()
		debug ('Got %d lines from subprocess %d' % (ct, process.pid))

		# send a new set of lines to each subprocess, ending each set
		# with a special OVER flag.

		if buckets[i]:
			for line in buckets[i]:
				process.stdin.write(line)
			process.stdin.flush()

			debug ('Sent %d lines to subprocess %d' % (
				len(buckets[i]), process.pid) )

		process.stdin.write (OVER + '\n')
		process.stdin.flush()

		# reset this bucket

		buckets[i] = []
		i = i + 1
	return

def parentMain():
	# start up our worker processes, with one bucket for each
	# (bucket[i] will hold the input lines for processes[i])

	processes = []
	for i in range(0, SUBPROC_COUNT):
		proc = subprocess.Popen ( [ sys.argv[0], '-x' ],
			stdin=subprocess.PIPE, stdout=subprocess.PIPE,
			bufsize=600000)
		processes.append (proc)
	buckets = []
	for i in range(0, SUBPROC_COUNT):
		buckets.append ([])

	debug ('Started %d subprocesses' % SUBPROC_COUNT)

	# read input lines, group them into buckets, and send them to
	# subprocesses for cleaning.  get the cleaned lines back and write
	# them out.

	cacheSize = 1000		# number of lines per batch
	bucketSize = cacheSize / SUBPROC_COUNT
	maxBucket = SUBPROC_COUNT - 1

	i = 0
	ct = 0
	pn = 0
	recordEnded = True

	line = sys.stdin.readline()
	while line:
		if (len(buckets[pn]) >= bucketSize) and recordEnded:
			if pn < maxBucket:
				pn = pn + 1
				debug ('filling bucket %d' % pn)

		buckets[pn].append (line)

		i = i + 1
		ct = ct + 1

		if ct >= cacheSize:
			subprocIO (buckets, processes)
			debug ('Total lines sent: %d' % i)
			ct = 0
			pn = 0

		if line[-4:-1] == '#=#':
			recordEnded = True
		else:
			recordEnded = False

		line = sys.stdin.readline() 
	debug ('Finished reading data: %d lines' % i)

	# if any extra lines, send them

	if ct > 0:
		debug ('Sending %d extra lines' % ct)
		subprocIO (buckets, processes)

	# gather final results

	debug ('Getting final results')
	subprocIO (buckets, processes)

	# terminate subprocesses

	for process in processes:
		process.stdin.write (QUIT + '\n')
		process.stdin.flush()
		process.stdin.close()
		process.wait()
		debug ('Subprocess %d ended' % process.pid)

	debug ('Main program ended') 
	return

def main():
	if '-x' in sys.argv:		# is a subprocess
		childMain()
	else:				# is the main (parent) process
		parentMain()
	return

if __name__ == '__main__':
	main()
