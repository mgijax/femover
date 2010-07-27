# Module: Dispatcher.py
# Purpose: to ease the process of working with subprocesses, as long as those
#	subprocesses do not require input via stdin
# Author: jsb

# Examples:  Say you have a script, foo, that takes a range of keys (start and
#	stop) as parameters and does some reporting for a particular type of
#	database object with that range of keys.  We know that there are many
#	of these keys, so we decide to work in parallel to speed things up.
#
#	Let's use ranges of 10,000 values for these keys, and the default
#	set of 5 simultaneous processes, as in:
#		ranges = [ (0, 10000), (10001, 20000), ... (490000, 500000) ]
#		dispatcher = Dispatcher ()
#		ids = []
#		for (start, stop) in ranges:
#			cmd = 'foo %d %d' % (start, stop)
#			ids.append (dispatcher.schedule (cmd))
#		dispatcher.wait()
#
#	Then, if you need to echo to stdout whatever the subprocesses had
#	written to their stdouts, you could do:
#		for id in ids:
#			sys.stdout.write (''.join(dispatcher.getStdout(id)))
#
#	If we didn't care about accessing the output from the subprocesses,
#	then we wouldn't need to commands' IDs and the call could be as
#	simple as:
#		cmds = map(lambda x : 'foo %d %d' % x, ranges)
#		Dispatcher(initialCommands = cmds).wait()
#
#	If we wanted to parallelize more and use 15 subprocesses, the last
#	line could be:
#		Dispatcher(15, cmds).wait()

import time
import types
import subprocess
import sys

###--------------------------------------------###
###--- status values for scheduled commands ---###
###--------------------------------------------###

UNKNOWN = 0		# command is unknown by this dispatcher
WAITING = 1		# command is waiting to be run
RUNNING = 2		# command is currently running in a subprocess
FINISHED = 3		# command has finished

###----------------------------------------------------###
###--- global variables related to debugging output ---###
###----------------------------------------------------###

DEBUG = False			# issue debugging output (True) or not?
START_TIME = time.time()	# initial time (in seconds) at load time

###-----------------###
###--- functions ---###
###-----------------###

def setDebug (
	mode = True	# boolean; set debugging mode (True) or not (False)
	):
	# Purpose: turn debugging output on or off
	# Returns: nothing
	# Assumes: nothing
	# Modifies: global variable 'DEBUG'
	# Throws: nothing
	# Notes: debugging output is off (False) initially

	global DEBUG
	DEBUG = mode
	return

###---------------###
###--- classes ---###
###---------------###

class Dispatcher:
	# Is: a dispatcher for managing subprocesses and commands waiting to
	#	be executed in subprocesses
	# Has: a list of commands, a list of running subprocesses, and a limit
	#	on the number of concurrent subprocesses to allow
	# Does: accepts commands to be executed, manages subprocesses to
	#	execute those commands, and provides access to the resulting
	#	stdout, stderr, and return code for each command once it has
	#	finished executing
	# Notes: This class does not run asynchronously within the Python
	#	interpreter, so it can only run when one of its methods is
	#	invoked.  It is at those moments that we call the
	#	__manageDispatchers() method to look for finished processes
	#	and to start up new ones for all instantiated Dispatchers.
	#	In comments, this is referred to simply as "housekeeping".

	###-----------------------###
	###--- class variables ---###
	###-----------------------###

	nextDispatcherID = 1	# integer; ID number to be assigned to the
				# ...next Dispatcher to be instantiated

	dispatchers = []	# list of all Dispatcher objects that have
				# ...been instantiated so far

	###-------------------###
	###--- constructor ---###
	###-------------------###

	def __init__ (self,
		maxProcesses = 5, 	# integer; max number of subprocesses
					# ...to manage concurrently
		initialCommands = []	# list of lists or strings; each inner
					# ...list or string represents one
					# ...command to be executed.  (If a
					# ...list, each parameter is its own
					# ...separate string in that list.)
		):
		# Purpose: constructor; build a Dispatcher
		# Returns: nothing
		# Assumes: nothing
		# Modifies: class variables 'nextDispatcherID' & 'dispatchers'
		# Throws: nothing

		self.nextID = 1			  # int; ID of next command
		self.lastStartedID = 0		  # int; ID of last command to
						  # ...be executed
		self.maxProcesses = maxProcesses  # int; max num of concurrent
						  # ...subprocesses
		self.activeProcesses = []	  # list of Popen objects; the
						  # ...currently executing
						  # ...subprocesses
		self.command = {}		  # maps from int ID to the
						  # ...corresponding command
		self.commandString = {}		  # maps from int ID to string
						  # ...version of the command
		self.status = {}		  # maps from int ID to the
						  # ...status of the command
						  # ...(WAITING, RUNNING, or
						  # ...FINISHED)
		self.stdout = {}		  # maps from int ID to the
						  # ...contents of stdout for
						  # ...that command
		self.stderr = {}		  # maps from int ID to the
						  # ...contents of stderr for
						  # ...that command
		self.returnCode = {}		  # maps from int ID to the
						  # ...return code for that
						  # ...command

		# grab an ID for this Dispatcher and advance the counter

		self.dispatcherID = Dispatcher.nextDispatcherID
		Dispatcher.nextDispatcherID = 1 + Dispatcher.nextDispatcherID
		self.debug ('initialized')

		# if we were given an initial list of commands to run, go
		# ahead and schedule them

		if initialCommands:
			if type(initialCommands[0]) == types.StringType:
				initialCommands = [ initialCommands ]
			for cmd in initialCommands:
				self.schedule (cmd)

		# add this one to the list of Dispatchers to be managed

		Dispatcher.dispatchers.append (self)
		return

	###------------------------###
	###--- instance methods ---###
	###------------------------###

	def debug (self,
		message		# string; message to write to stderr
		):
		# Purpose: write a date-stamped debugging message to stderr,
		#	if the module is set for debugging mode
		# Returns: nothing
		# Assumes: we can write to stderr
		# Modifies: writes to stderr
		# Throws: an exception only if we cannot write to stderr

		if DEBUG:
			sys.stderr.write('%10.3f : dispatcher %d : %s\n' % (
				time.time() - START_TIME,
				self.dispatcherID,
				message))
		return

	def getActiveProcessCount (self):
		return len(self.activeProcesses)

	def getWaitingProcessCount (self):
		i = 0
		for v in self.status.values():
			if v == WAITING:
				i = i + 1
		return i

	def schedule (self,
		argv		# list or string; the command to be executed
				# ...(if a list, it should contain separate
				# ...strings for the program name and its
				# ...parameters)
		):
		# Purpose: schedule the given command for execution
		# Returns: integer ID assigned for this command
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		# if we were given a string, split it into a list

		if type(argv) == types.StringType:
			argv = argv.split(' ')

		# add to the list of commands and their string representations

		self.command[self.nextID] = argv
		self.commandString[self.nextID] = ' '.join (map (str, argv))

		# intialize dictionaries for the attributes of this command

		self.status[self.nextID] = WAITING
		self.stdout[self.nextID] = None
		self.stderr[self.nextID] = None
		self.returnCode[self.nextID] = None

		# advance the counter to get ready for the next command

		self.nextID = self.nextID + 1
		self.debug ('scheduled command: %s' % ' '.join (map
			(str, argv) ) )

		self.__manageDispatchers()	# housekeeping
		return self.nextID - 1

	def getStatus (self,
		id		# integer; ID for the desired command
		):
		# Purpose: retrieve the status for the desired command
		# Returns: integer status value, from one of the options at
		#	the top of the module: UNKNOWN, WAITING, RUNNING, or
		#	FINISHED
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		self.__manageDispatchers()	# housekeeping
		if not self.status.has_key(id):
			return UNKNOWN
		return self.status[id]

	def getStdout (self,
		id		# integer; ID for the desired command
		):
		# Purpose: retrieve stdout for the desired command
		# Returns: list of strings from the command's stdout.  Each
		#	string terminates with a newline character.  If the
		#	command has not finished, or if the 'id' is unknown,
		#	then this method returns None.
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		self.__manageDispatchers()	# housekeeping
		if not self.stdout.has_key(id):
			return None
		return self.stdout[id]

	def getStderr (self,
		id		# integer; ID for the desired command
		):
		# Purpose: retrieve stderr for the desired command
		# Returns: list of strings from the command's stderr.  Each
		#	string terminates with a newline character.  If the
		#	command has not finished, or if the 'id' is unknown,
		#	then this method returns None.
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		self.__manageDispatchers()	# housekeeping
		if not self.stderr.has_key(id):
			return None
		return self.stderr[id]

	def getReturnCode (self,
		id		# integer; ID for the desired command
		):
		# Purpose: retrieve the return code for the desired command
		# Returns: integer return code from the command, or None if
		#	either the command has not finished or if the 'id' is
		#	unknown.
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		self.__manageDispatchers()	# housekeeping
		if not self.returnCode.has_key(id):
			return None
		return self.returnCode[id]

	def __manageSubprocesses (self):
		# Purpose: (private) manage the subprocesses and waiting
		#	commands for this Dispatcher
		# Returns: nothing
		# Assumes: nothing
		# Modifies: does wait() calls to release any completed
		#	subprocesses; starts new subprocesses
		# Throws: nothing 

		# close out finished processes

		for (process, id) in self.activeProcesses:

			# if the poll() method returns non-None, then the
			# subprocess has finished

			if process.poll() != None:
				# read return code, stdout, stderr

				self.stdout[id] = process.stdout.readlines()
				self.stderr[id] = process.stderr.readlines()
				self.returnCode[id] = process.returncode
				self.status[id] = FINISHED

				# let the process terminate, then remove it
				# from the list of active processes

				process.wait()
				self.activeProcesses.remove( (process, id) )
				self.debug ('finished command: %s' % \
					self.commandString[id])

		# if we have more commands waiting to be started, then we can
		# start new subprocesses for them (up to the limit on the
		# number of subprocesses)

		while (self.nextID > (self.lastStartedID + 1)) and \
			(len(self.activeProcesses) < self.maxProcesses):

				self.debug ('starting command: %s' % \
				    self.commandString[self.lastStartedID + 1])

				# move on to the next command to be started,
				# then start a new subprocess for it

				self.lastStartedID = self.lastStartedID + 1
				self.activeProcesses.append ( (
					subprocess.Popen (self.command[
						self.lastStartedID],
						stdout=subprocess.PIPE,
						stderr=subprocess.PIPE),
					self.lastStartedID) )

				# upgrade the status for that command

				self.status[self.lastStartedID] = RUNNING
				self.debug ('started command: %s' % \
				    self.commandString[self.lastStartedID])
		return

	def __manageDispatchers (self):
		# Purpose: (private) iterates over all Dispatchers to give
		#	them a chance to release finished processes and to
		#	start up new ones
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		for dispatcher in Dispatcher.dispatchers:
			dispatcher.__manageSubprocesses()
		return

	def wait (self,
		callback = None		# function/method to be called
					# ...periodically while waiting (to
					# ...enable reporting)
		):
		# Purpose: wait for all subprocesses of this Dispatcher to
		#	finish
		# Returns: nothing
		# Assumes: nothing
		# Modifies: nothing
		# Throws: nothing

		# We use manageDispatchers here instead of manageSubprocesses
		# because we want other Dispatchers to continue working
		# through their commands as well.

		# finish old processes, start new
		self.__manageDispatchers()

		# as long as we still have active processes, wait a brief
		# time and then check again

		while self.activeProcesses:
			time.sleep(0.5)
			self.__manageDispatchers()
			if callback:
				callback()

		self.debug ('all commands finished')
		return

###-----------------------------------------###
###--- main program (for module testing) ---###
###-----------------------------------------###

if __name__ == '__main__':
	# use sleep commands of various lengths to test the timing mechanism,
	# then use an 'echo' command to test the capturing of stdout

	cmds = []
	for i in range(1,9):
		cmds.append ( [ 'sleep', str(i) ] )
	cmds.append ('echo hello')

	# turn on debugging, then use Dispatchers to manage 3 and 5 concurrent
	# subprocesses

	setDebug(True)
	d3 = Dispatcher(3, cmds)
	d5 = Dispatcher(5, cmds)

	# also use a Dispatcher to manage 8 subprocesses, but this time add
	# the commands individually so we can catch their ID numbers and use
	# them to request reporting information later on

	ids = []
	d8 = Dispatcher(8)
	for cmd in cmds:
		ids.append (d8.schedule(cmd))

	# wait for all the subprocesses to finish

	d3.wait()
	d5.wait()
	d8.wait()

	# do reporting on those commands issued through the 8-way Dispatcher

	for id in ids:
		print 'Stdout for command %d' % id
		print d8.getStdout (id)
		print 'Stderr for command %d' % id
		print d8.getStderr (id)
		print 'Return code for command %d' % id
		print d8.getReturnCode (id)
