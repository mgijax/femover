#!/usr/local/bin/python
# 
# gathers data for the mapping_table, mapping_table_row, and mapping_table_cell tables in the front-end database

import Gatherer
import logger

###--- Globals ---###

TABLES = 'mapping_table'		# output filenames / names of tables generated
ROWS = 'mapping_table_row'
CELLS = 'mapping_table_cell'

CROSS_MATRIX = 'CROSS MATRIX'	# table types
CROSS_2x2 = 'CROSS 2x2'
CROSS_STATISTICS = 'CROSS STATISTICS'

HEADER = 1						# flag for header vs. data row
DATA = 0

CROSS_MATRIX_HEADER = 'MC #mice'	# leftmost cell of CROSS matrix header row

###--- Classes ---###

class MappingTableGatherer (Gatherer.CachingMultiFileGatherer):
	# Is: a data gatherer for the mapping_table, mapping_table_row, and mapping_table_cell tables
	# Has: queries to execute against the source database
	# Does: queries the source database for data to be displayed in various types of tables for mapping
	#	experiments, collates results, writes tab-delimited text files

	def preprocessCommands(self):
		# intended to tweak SQL commands, but repurposing it to set a couple object variables
		
		self.tableKeys = {}			# experiment key -> table type -> table key
		self.maxRowKey = 0
		self.maxTableKey = 0
		return

	def getNewRowKey(self):
		# return the next available tableKey
		
		self.maxRowKey = self.maxRowKey + 1
		return self.maxRowKey
	
	def getNewTableKey(self):
		# return the next available tableKey
		
		self.maxTableKey = self.maxTableKey + 1
		return self.maxTableKey
	
	def getTableKey(self, experimentKey, tableType):
		# get the table key for the experiment / type pair, assigning a new one if needed

		if experimentKey not in self.tableKeys:
			self.tableKeys[experimentKey] = {}
		if tableType not in self.tableKeys[experimentKey]:
			self.tableKeys[experimentKey][tableType] = self.getNewTableKey()
		return self.tableKeys[experimentKey][tableType]
			
	
	def processCrossMatrixHeaders(self):
		# process results of the query 0, building header rows for CROSS matrix displays

		# header rows for matrix data
		cols0, rows0 = self.results[0]
		
		exptKeyCol = Gatherer.columnNumber(cols0, '_Expt_key')
		symbolCol = Gatherer.columnNumber(cols0, 'symbol')
		idCol = Gatherer.columnNumber(cols0, 'markerID')

		tablesCreated = {}
		cellSeqNum = 0
		rowKey = None
		
		for row in rows0:
			exptKey = row[exptKeyCol]
			tableKey = self.getTableKey(exptKey, CROSS_MATRIX)

			if tableKey not in tablesCreated:
				# create table record
				self.addRow(TABLES, [ tableKey, exptKey, CROSS_MATRIX, tableKey ])
				
				# create header row
				rowKey = self.getNewRowKey()
				self.addRow(ROWS, [ rowKey, tableKey, HEADER, rowKey ])
				
				# add first column
				cellSeqNum = 1
				self.addRow(CELLS, [ rowKey, None, CROSS_MATRIX_HEADER, cellSeqNum ])
				
				tablesCreated[tableKey] = True
				
			# add new column to table
			cellSeqNum = cellSeqNum + 1
			self.addRow(CELLS, [ rowKey, row[idCol], row[symbolCol], cellSeqNum ])
		
		return
	
	def processCrossMatrixDataRows(self):
		# process results of the query 1, building data rows for CROSS matrix displays

		cols1, rows1 = self.results[1]
		
		exptKeyCol = Gatherer.columnNumber(cols1, '_Expt_key')
		allelesCol = Gatherer.columnNumber(cols1, 'alleleline')
		offspringCol = Gatherer.columnNumber(cols1, 'offspringNmbr')

		for row in rows1:
			exptKey = row[exptKeyCol]
			tableKey = self.getTableKey(exptKey, CROSS_MATRIX)
			
			# create new row
			rowKey = self.getNewRowKey()
			self.addRow(ROWS, [ rowKey, tableKey, DATA, rowKey ])

			# populate new row with cells (first is offspring count, others are alleles)
			cellSeqNum = 1
			self.addRow(CELLS, [ rowKey, None, row[offspringCol], cellSeqNum ])
			
			for allele in row[allelesCol].split(' '):
				cellSeqNum = cellSeqNum + 1
				self.addRow(CELLS, [ rowKey, None, allele, cellSeqNum ])
		return
			
	def processCross2x2Data(self):
		# process results of the query 2, 2x2 data for CROSS displays

		cols2, rows2 = self.results[2]
		
		exptKeyCol = Gatherer.columnNumber(cols2, '_Expt_key')
		symbol1Col = Gatherer.columnNumber(cols2, 'marker1')
		markerID1Col = Gatherer.columnNumber(cols2, 'markerID1')
		symbol2Col = Gatherer.columnNumber(cols2, 'marker2')
		markerID2Col = Gatherer.columnNumber(cols2, 'markerID2')
		recombinantsCol = Gatherer.columnNumber(cols2, 'numRecombinants')
		parentalsCol = Gatherer.columnNumber(cols2, 'numParentals')

		tablesCreated = {}
		rowKey = None
		
		for row in rows2:
			exptKey = row[exptKeyCol]
			tableKey = self.getTableKey(exptKey, CROSS_2x2)

			if tableKey not in tablesCreated:
				# create table record
				self.addRow(TABLES, [ tableKey, exptKey, CROSS_2x2, tableKey ])
				
				# create header row
				rowKey = self.getNewRowKey()
				self.addRow(ROWS, [ rowKey, tableKey, HEADER, rowKey ])
				
				# add standard column headers
				self.addRow(CELLS, [ rowKey, None, 'Marker 1', 1 ])
				self.addRow(CELLS, [ rowKey, None, 'Marker 2', 2 ])
				self.addRow(CELLS, [ rowKey, None, '# Recombinants', 3 ])
				self.addRow(CELLS, [ rowKey, None, '# Parentals', 4 ])
				
				tablesCreated[tableKey] = True
				
			# create new row
			rowKey = self.getNewRowKey()
			self.addRow(ROWS, [ rowKey, tableKey, DATA, rowKey ])

			# populate new row with cells
			self.addRow(CELLS, [ rowKey, row[markerID1Col], row[symbol1Col], 1 ])
			self.addRow(CELLS, [ rowKey, row[markerID2Col], row[symbol2Col], 2 ])
			self.addRow(CELLS, [ rowKey, None, row[recombinantsCol], 3 ])
			self.addRow(CELLS, [ rowKey, None, row[parentalsCol], 4 ])
		return

	def processCrossStatistics(self):
		# process results of the query 3, statistics data for CROSS displays

		cols3, rows3 = self.results[3]
		
		exptKeyCol = Gatherer.columnNumber(cols3, '_Expt_key')
		symbol1Col = Gatherer.columnNumber(cols3, 'marker1')
		markerID1Col = Gatherer.columnNumber(cols3, 'markerID1')
		symbol2Col = Gatherer.columnNumber(cols3, 'marker2')
		markerID2Col = Gatherer.columnNumber(cols3, 'markerID2')
		recombinantsCol = Gatherer.columnNumber(cols3, 'recomb')
		totalCol = Gatherer.columnNumber(cols3, 'total')
		percentCol = Gatherer.columnNumber(cols3, 'pcntrecomb')
		stderrCol = Gatherer.columnNumber(cols3, 'stderr')

		tablesCreated = {}
		rowKey = None
		
		for row in rows3:
			exptKey = row[exptKeyCol]
			tableKey = self.getTableKey(exptKey, CROSS_STATISTICS)

			if tableKey not in tablesCreated:
				# create table record
				self.addRow(TABLES, [ tableKey, exptKey, CROSS_STATISTICS, tableKey ])
				
				# create header row
				rowKey = self.getNewRowKey()
				self.addRow(ROWS, [ rowKey, tableKey, HEADER, rowKey ])
				
				# add standard column headers
				self.addRow(CELLS, [ rowKey, None, 'Marker 1', 1 ])
				self.addRow(CELLS, [ rowKey, None, 'Marker 2', 2 ])
				self.addRow(CELLS, [ rowKey, None, '# Recombinants', 3 ])
				self.addRow(CELLS, [ rowKey, None, 'Total', 4 ])
				self.addRow(CELLS, [ rowKey, None, '% Recombinants', 4 ])
				self.addRow(CELLS, [ rowKey, None, 'Std Error', 4 ])
				
				tablesCreated[tableKey] = True
				
			# create new row
			rowKey = self.getNewRowKey()
			self.addRow(ROWS, [ rowKey, tableKey, DATA, rowKey ])

			# populate new row with cells
			self.addRow(CELLS, [ rowKey, row[markerID1Col], row[symbol1Col], 1 ])
			self.addRow(CELLS, [ rowKey, row[markerID2Col], row[symbol2Col], 2 ])
			self.addRow(CELLS, [ rowKey, None, row[recombinantsCol], 3 ])
			self.addRow(CELLS, [ rowKey, None, row[totalCol], 4 ])
			self.addRow(CELLS, [ rowKey, None, '%0.3f' % row[percentCol], 5 ])
			self.addRow(CELLS, [ rowKey, None, '%0.3f' % row[stderrCol], 6 ])
		return

	def processCrossData(self):
		# process results of the queries for CROSS mapping experiments (queries 0-3)
		self.processCrossMatrixHeaders()
		self.processCrossMatrixDataRows()
		self.processCross2x2Data()
		self.processCrossStatistics()
		return

	def collateResults(self):
		self.processCrossData()
		return
	
###--- globals ---###

cmds = [
	# 0. marker data for CROSS experiments -- used for header rows of Experiment Matrix Data table
	'''select g._Expt_key, l.symbol, a.accID as markerID
		from MLD_Expts e, MLD_Expt_Marker g, MRK_Marker l, ACC_Accession a
		where e._Expt_key = g._Expt_key
			and e.exptType = 'CROSS'
			and l._Marker_key = g._Marker_key
			and matrixData = 1 
			and l._Marker_key = a._Object_key
			and a._MGIType_key = 2
			and a.preferred = 1
			and a._LogicalDB_key = 1
			and g._Expt_key >= %d
			and g._Expt_key < %d
			and exists (select 1 from MLD_MCDataList d where e._Expt_key = d._Expt_key)
		order by g._Expt_key, g.sequenceNum''',
	
	# 1. matrix data rows for CROSS experiments
	'''select d._Expt_key, d.sequenceNum, d.alleleLine, d.offspringNmbr
		from MLD_MCDataList d, MLD_Expts e
		where d._Expt_key >= %d
			and d._Expt_key < %d
			and d._Expt_key = e._Expt_key
			and e.exptType = 'CROSS'
		order by d._Expt_key, d.sequenceNum''',
		
	# 2. 2x2 data rows for CROSS experiments
	'''select m._Expt_key, l1.symbol as marker1, a1.accID as markerID1, l2.symbol as marker2,
			a2.accID as markerID2, numRecombinants, numParentals
		from MLD_MC2point m, MRK_Marker l1, MRK_Marker l2, ACC_Accession a1, ACC_Accession a2, MLD_Expts e
		where m._Marker_key_1 = l1._Marker_key
			and m._Expt_key = e._Expt_key
			and e.exptType = 'CROSS'
			and m._Marker_key_2 = l2._Marker_key
			and l1._Marker_key = a1._Object_key
			and a1._MGIType_key = 2
			and a1._LogicalDB_key = 1
			and a1.preferred = 1
			and l2._Marker_key = a2._Object_key
			and a2._MGIType_key = 2
			and a2._LogicalDB_key = 1
			and a2.preferred = 1
			and m._Expt_key >= %d
			and m._Expt_key < %d
		order by m._Expt_key, m.sequenceNum''',
		
	# 3. statistics table rows for CROSS experiments
	'''select s._Expt_key, s.sequenceNum, l1.symbol as marker1, l2.symbol as marker2, a1.accID as markerID1,
			a2.accID as markerID2, s.recomb, s.total, s.pcntrecomb, s.stderr
		from MLD_Statistics s, MRK_Marker l1, MRK_Marker l2, MLD_Expts e, ACC_Accession a1, ACC_Accession a2
		where s._Marker_key_1 = l1._Marker_key
			and s._Marker_key_2 = l2._Marker_key
			and s._Expt_key = e._Expt_key
			and e.exptType = 'CROSS'
			and l1._Marker_key = a1._Object_key
			and a1._MGIType_key = 2
			and a1._LogicalDB_key = 1
			and a1.preferred = 1
			and l2._Marker_key = a2._Object_key
			and a2._MGIType_key = 2
			and a2._LogicalDB_key = 1
			and a2.preferred = 1
			and s._Expt_key >= %d
			and s._Expt_key < %d
		order by s._Expt_key, s.sequenceNum''',
	]

files = [
		(TABLES,
			[ 'mapping_table_key', '_Expt_key', 'table_type', 'sequenceNum' ],
			[ 'mapping_table_key', '_Expt_key', 'table_type', 'sequenceNum' ] ),
		(ROWS,
			[ 'row_key', 'mapping_table_key', 'is_header', 'sequenceNum' ],
			[ 'row_key', 'mapping_table_key', 'is_header', 'sequenceNum' ] ),
		(CELLS,
			[ 'row_key', 'markerID', 'label', 'sequenceNum' ],
			[ Gatherer.AUTO, 'row_key', 'markerID', 'label', 'sequenceNum' ] ),
	]

# global instance of a MappingTableGatherer
gatherer = MappingTableGatherer (files, cmds)
gatherer.setupChunking (
	"select min(_Expt_key) from MLD_Expts where exptType != 'CONTIG' ",
	"select max(_Expt_key) from MLD_Expts where exptType != 'CONTIG' ",
	10000
	)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
