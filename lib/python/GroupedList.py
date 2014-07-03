import time
import sys

class GroupedList:
	# Is: a set of unique items
	# Has: a set of unique items
	# Does: allows items to be added to the set and retrieved from the set
	# Notes: This class exists to strike a balance between performance and
	#	memory requirements.  There are cases where we would like to
	#	use a Python dictionary for speed of access, but dictionaries
	#	are much more memory intensive than lists (7-9x the memory for
	#	large sets of items).  Lists use much less memory, but are
	#	also much slower to test for whether an item is contained
	#	(based on the number of items; eg- 7,300x as bad for 100,000
	#	items).  Python's new "set" data type (as of Python 2.4) is
	#	between dictionaries and lists for memory usage, but is even
	#	slower than lists to test for membership.  And, so comes the
	#	need for our GroupedList.  We split a logical list up into
	#	sublists, so we have a much smaller list to iterate over to
	#	test for membership.  The memory requirements are very close
	#	to those for a list, while the performance is much better
	#	(roughly 98% faster) than either a list or a set.  Still not
	#	anywhere close to dictionary performance, but at least closer.

	def __init__ (self,
		numberOfGroups = 100,	# how many subgroups to use
		items = []		# initial list of items
		):
		# constructor

		self.numberOfGroups = numberOfGroups
		self.groups = {}
		self.groupNumbers = []

		for item in items:
			self.add(item)
		return

	def add (self,
		item		# hashable item to add to the GroupedList
		):
		# Purpose: add an 'item' to the GroupedList.  If it is already
		#	in GroupedList, do not add another copy.

		group = hash(item) % self.numberOfGroups
		if not self.groups.has_key(group):
			self.groups[group] = [ item ]
			self.groupNumbers.append (group)
		elif item not in self.groups[group]:
			self.groups[group].append (item)
		return

	def contains (self,
		item		# hashable item to test for membership
		):
		# Purpose: test if 'item' exists in the GroupedList.
		# Returns: True if it is in the GroupedList, False if not.

		group = hash(item) % self.numberOfGroups
		if self.groups.has_key(group):
			if item in self.groups[group]:
				return True
		return False

	def __len__ (self):
		# Purpose: find out how many items are in the GroupedList
		# Returns: integer number of items

		size = 0
		for group in self.groupNumbers:
			size = size + len(self.groups[group])
		return size

	def items (self):
		# Purpose: get the full set of items contained in this object
		# Returns: list

		allItems = []
		for group in self.groupNumbers:
			allItems = allItems + self.groups[group]
		return allItems

	def __getitem__ (self,
		index		# integer; 
		):
		# Purpose: return the item in the GroupedList identified by
		#	this integer index
		# Returns: a single item from this GroupedList identified by
		#	this integer index
		# Notes: This 'index' does not forever identify the same item.
		#	If you add a new item to the GroupedList, then the
		#	index will likely refer to a different item.  This can
		#	be used in concert with len() to iterate over the
		#	items that are contained at a given point in time.
		# Example:
		#	i = 0
		#	while i < len(myGroupedListObject):
		#		item = myGroupedListObject[i]
		#		print item
		#		i = i + 1

		soFar = -1
		for group in self.groupNumbers:
			groupSize = len(self.groups[group])
			if (soFar + groupSize) >= index:
				return self.groups[group][index - soFar - 1]
			soFar = soFar + groupSize
		return None

	def sizeInBytes (self):
		# Purpose: find out how much memory this GroupedList uses
		# Returns: integer number of bytes used by this GroupedList
		#	and the items it contains

		size = sys.getsizeof(self.numberOfGroups) + \
			sys.getsizeof(self.groupNumbers)

		for group in self.groupNumbers:
			size = size + sys.getsizeof(self.groups[group]) \
				+ sys.getsizeof(group) 
		return size
