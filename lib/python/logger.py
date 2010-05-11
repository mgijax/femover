import os
import sys
import time
import config

###--- Globals ---###

DEBUG = config.LOG_DEBUG		# boolean; log debug messages?
INFO = config.LOG_INFO			# boolean; log informational messages?
LAST_TIME = time.time()			# float; time of last logged message
NAME = os.path.basename (sys.argv[0])	# string; name to pre-pend to messages

###--- Functions ---###

def getName():
	# Purpose: get the name currently in use as a prefix for messages
	# Returns: string
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	return NAME

def setName (
	name		# string; name to pre-pend to log messages
	):
	# Purpose: set the name to use as a prefix for messages
	# Returns: string
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	global NAME
	NAME = name
	return

def getDebug():
	# Purpose: get the current debug setting
	# Returns: boolean (True if debugging, False if not)
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	return DEBUG

def setDebug (
	debugOn = True		# boolean; True to turn debug messages on
	):
	# Purpose: turn debug logging on (True) or off (False)
	# Returns: nothing
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	global DEBUG
	DEBUG = debugOn
	return

def getInfo():
	# Purpose: get the current informational message setting
	# Returns: boolean (True if logging info messages, False if not)
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	return INFO

def setInfo (
	infoOn = True		# boolean; True to turn info messages on
	):
	# Purpose: turn informational logging on (True) or off (False)
	# Returns: nothing
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	global INFO
	INFO = infoOn
	return

def info (
	message			# string; informational message to be logged
	):
	# Purpose: write 'message' to the log, if we are currently logging
	#	informational messages
	# Returns: nothing
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	if INFO:
		__output ('info', message)
	return

def debug (
	message			# string; debug message to be logged
	):
	# Purpose: write 'message' to the log, if we are currently logging
	#	debugging messages
	# Returns: nothing
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	if DEBUG:
		__output ('debug', message)
	return

def error (
	message			# string; error message to be logged
	):
	# Purpose: write 'message' to the log
	# Returns: nothing
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing
	# Notes: We always log error messages; there is no way to turn them
	#	off.

	__output ('error', message)
	return

def __output (
	messageType,		# string; type of message being logged
	message			# string; message to be logged
	):
	# Purpose: write 'message' of the given 'messageType' to stderr
	# Returns: nothing
	# Assumes: We can write to stderr.
	# Modifies: writes to stderr; updates global LAST_TIME
	# Throws: nothing

	global LAST_TIME

	now = time.time()
	elapsed = now - LAST_TIME
	LAST_TIME = now

	sys.stderr.write ('%s : %s : %6.3f sec : %s\n' % (
		NAME, messageType, elapsed, message))
	return

