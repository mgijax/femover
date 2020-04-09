# Module: MPSorter.py
# Purpose: to provide a unified means of calculating sorts and depths for MP based on the DAG

import dbAgnostic
import logger

class MPSorter:
	DAG_MAP = {}

	###--- "DAG"gy functions ---###
	# iterates the DAG query and puts the hierarchy into a giant lookup
	def getMPDAG(self):
		if self.DAG_MAP:
			return self.DAG_MAP 
		logger.info("initialising map of MP DAG terms")
		dagQuery = """
			select n1._object_key parent_key,
			vt1.term parent, 
			n2._object_key child_key,
			vt2.term child 
			from dag_edge e, 
			dag_node n1, 
			dag_node n2, 
			voc_term vt1, 
			voc_term vt2
			where e._dag_key=4
				and e._child_key=n2._node_key
				and e._parent_key = n1._node_key
				and n1._object_key = vt1._term_key
				and n2._object_key = vt2._term_key
			order by e._parent_key, vt2.term
		"""
		cols,rows = dbAgnostic.execute(dagQuery)
		count = 0
		# init the dag tree
		self.DAG_MAP = {}
		for row in rows:
			count += 1
			parentKey = row[0]
			childKey = row[2]
			self.DAG_MAP.setdefault(parentKey,[]).append(childKey)

		logger.info("done initialising map of MP DAG terms--testing")
		return self.DAG_MAP 

	# recurse through the MP dag to figure out how to sort (and indent) a subset of terms for a given system (header)
	# incoming terms are of the format [(termKey,termSeq),]
	# returns a map of termKey : {"seq": seq, "depth": depth}
	def calculateSortAndDepths(self,terms,rootKey):
		# sort term keys by term_seq
		sortMap = {}
		for termKey,termSeq in terms:
			# define a default depth and "new_seq"
			# new_seq is a tuple that will be sorted
			sortMap[termKey] = {"termKey":termKey,"seq":termSeq,"depth":1,"new_seq":(1,),"set":False}
		# set defaults for the root header term key
		if rootKey not in sortMap:
			sortMap[rootKey] = {"termKey":rootKey,"new_seq":(1,),"depth":1}
		# recurse through the dag to calculate the sorts (and depths)
		sortMap = self.recurseSorts(sortMap,[x[0] for x in terms],rootKey)

		# now sort by parent_seq(s), then term_seq (is a tuple like (parent1_seq,parent2_seq, etc, term_seq)
		sortedTerms = sorted(sortMap.values(), key=lambda x: x["new_seq"])
		# normalise the sorts relative to this system
		count=0
		for value in sortedTerms:
			count += 1
			sortMap[value["termKey"]] = {"seq":count,"depth":value["depth"]}
		return sortMap

	# recursive function to traverse dag and calculate sorts and depths for the given term keys
	# expects a sortMap as defined above, list of termKeys, system term key 
	# returns the original sortMap with modified values for "new_seq" and "depth"
	def recurseSorts(self,sortMap,termKeys,rootKey):
		dagMap = self.getMPDAG()
		stack = [(rootKey,(1,),1)]
		
		while stack:
			rootKey, parentSeq, depth = stack.pop()
			if rootKey in dagMap:
				for childKey in dagMap[rootKey]:
					# check if this key is one in our list, then check if it has been set before, if it has,
					# also check if the depth is less than what we want to set it to (we pick the longest annotated path)
					if (childKey in termKeys) and \
						((not sortMap[childKey]["set"]) or (sortMap[childKey]["depth"] < depth)):
						sortMap[childKey]["set"] = True
						# perform tuple concatenation on parent seq
						# we build a sortable tuple like (parent1_seq,parent2_seq,etc,term_seq)
						childSeq = parentSeq + (sortMap[childKey]["seq"],)
						sortMap[childKey]["new_seq"] = childSeq
						# set the depth for this term
						sortMap[childKey]["depth"] = depth
						# recurse with new depth and term_seq info
						stack.append((childKey,childSeq,depth+1))
					else:
						# recurse further into dag with current depth and parent_seq info
						stack.append((childKey,parentSeq,depth))
		return sortMap
	"""
	Gained a couple minutes of performance advantage by switching from recursion to stack based method
	def recurseSorts(self,sortMap,termKeys,rootKey,parentSeq=(1,),depth=1):
		dagMap = self.getMPDAG()
		if rootKey in dagMap:
			for childKey in dagMap[rootKey]:
				# check if this key is one in our list, then check if it has been set before, if it has,
				# also check if the depth is less than what we want to set it to (we pick the longest annotated path)
				if (childKey in termKeys) and \
					((not sortMap[childKey]["set"]) or (sortMap[childKey]["depth"] < depth)):
					sortMap[childKey]["set"] = True
					# perform tuple concatenation on parent seq
					# we build a sortable tuple like (parent1_seq,parent2_seq,etc,term_seq)
					childSeq = parentSeq + (sortMap[childKey]["seq"],)
					sortMap[childKey]["new_seq"] = childSeq
					# set the depth for this term
					sortMap[childKey]["depth"] = depth
					# recurse with new depth and term_seq info
					self.recurseSorts(sortMap,termKeys,childKey,childSeq,depth+1)
				else:
					# recurse further into dag with current depth and parent_seq info
					self.recurseSorts(sortMap,termKeys,childKey,parentSeq,depth)
		return sortMap
	"""

if __name__=="__main__":
	mpSorter = MPSorter()
	import unittest
	class MPSortTestCase(unittest.TestCase):
		def test_mp_sort(self):
			termKey = 50482
			termSeq = 358 
			termKey2 = 2261084
			termSeq2 = 104
			headerKey = 83347
			sortMap = mpSorter.calculateSortAndDepths([(termKey,termSeq),(termKey2,termSeq2)],headerKey) 
			print sortMap	
			termKey2 = 208844
			termSeq2 = 456
			sortMap = mpSorter.calculateSortAndDepths([(termKey,termSeq),(termKey2,termSeq2)],headerKey) 
			print sortMap	
	unittest.main()
