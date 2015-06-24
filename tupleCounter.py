
# coding: utf-8

# # Testing your installation 

# If you have installed correctly dispel4py, you can easily run this first test called pipeline_test.py  This is a dispel4py graph which produces a pipeline workflow with one producer node (prod) and 5 consumer nodes. 

# Remenber that first you need to have installed dispel4py:
# 
#     >> pip install dispel4py
#     
# For running this example:     

# In[4]:

#get_ipython().system(u'dispel4py simple dispel4py.examples.graph_testing.pipeline_test -i 10')


# Now, lets test your installation with the example that you have followed during the presentation (slides - Tutorial dispel4py basic I): 

# #### Check prime number

# In[8]:

from dispel4py.base import ProducerPE, IterativePE, ConsumerPE, GenericPE
from dispel4py.workflow_graph import WorkflowGraph
import string
import numpy as np
from collections import defaultdict
from multiprocessing import Process, freeze_support
import time

OUTPUT_FILELOG = "C:\\Users\\ry\\Desktop\\log.txt"

class TupleProducer(ProducerPE):
	def __init__(self, inputFile, numRepeats):
		ProducerPE.__init__(self)
		self.file = inputFile
		self.reps = numRepeats

	def simpleLogger(self, msg):
		try:
			print "%s (rank %s): %s" % (self.id, self.rank, msg)
		except:
			print "%s: %s" % (self.id, msg)

	def _process(self):
		
		for i in range(self.reps):
			myFH = open(self.file, 'r')
			excess = ""
			myFH.readline() #Gets rid of header
			for i, line in enumerate(myFH):
				output = string.strip(line)
				output = excess + output
				result = [output[jj:jj+3] for jj in range(0, len(output)-2, 3)]
				ee = len(output)%3
				if(ee > 0): excess = output[-1*ee:]
				else: excess = ''
				for res in result: 
					self.write('output', res)

			myFH.close()

class MakeDict(GenericPE):

	def __init__(self):
		GenericPE.__init__(self)
		self.myDict = defaultdict(int)
		self._add_input("input", grouping=[0])
		self._add_output("output")

	def simpleLogger(self, msg):
		try:
			print "%s (rank %s): %s" % (self.id, self.rank, msg)
		except:
			print "%s: %s" % (self.id, msg)

	def _process(self, tupl):
		#self.log("looking at tuple - %s " % tupl)
		t = tupl.values()[0]
		if t in self.myDict:
			self.myDict[t] += 1
		else:
			self.myDict[t] = 1

	def _postprocess(self):
		self.write('output', self.myDict.items())

class CollectCounts(GenericPE):

	def __init__(self, outputFileName):
		GenericPE.__init__(self)
		self._add_input("input", grouping=[0])
		self.outFile = outputFileName
		self.totalDict = defaultdict(int)

	def simpleLogger(self, msg):
		try:
			print "%s (rank %s): %s" % (self.id, self.rank, msg)
		except:
			print "%s: %s" % (self.id, msg)

	def _process(self, inputDict):
		sentDict = inputDict.values()[0]
		for elem in sentDict:
			k,v = elem
			if k in self.totalDict: self.totalDict[k] += v
			else: self.totalDict[k] = v

	def _postprocess(self):
		writeFH = open(self.outFile, 'w')
		for k, v in self.totalDict.iteritems():
			writeFH.write(k + " " + str(v) + "\n")


# In[9]:
#def tupleCounter():

def timer(timeList):
	timeList.append(time.time())
	if len(timeList)%2 == 0:
		print 'Time elapsed: ' + str(round(timeList[-1] - timeList[-2],4)) + ' seconds.'
		timeList.pop()
		timeList.pop()

def main():
	if len(sys.argv) < 5:
		print "Incorrect arguments provided. Proper format: python tupleCounter.py <inputFile> <numRepeats> <outputFile> <numCores>"
		sys.exit()
	inputFilename = sys.argv[1]
	numRepeats = int(sys.argv[2])
	outputFile = sys.argv[3]
	numCores = int(sys.argv[4])

	producer = TupleProducer(inputFilename, numRepeats)
	makeDicts = MakeDict()
	collector = CollectCounts(outputFile)

	graph = WorkflowGraph()
	graph.connect(producer, 'output', makeDicts, 'input')
	graph.connect(makeDicts, 'output', collector, 'input')

	from dispel4py.new.multi_process import process as multi_process
	import argparse

	args = argparse.Namespace
	args.num = numCores
	args.simple = False

	
	multi_process(graph, {producer: 1}, args)

if __name__ == '__main__':
	timeList = []
	timer(timeList)
	main()
	timer(timeList)

# Running the workflow in the notebook by using a handy function called "simple_process":
# This function allows us to run a graph, by indicanting the first PE, and the number of iterations that we want to run it. e.g. 
#  * simple_process(graph, {producer: 1}) 
# 
# It runs 1 iteration of the graph
# 

# In[10]:

#from dispel4py.new.simple_process import process as simple_process
#simple_process(graph, {producer: 1})
#c:\users\ry\appdata\local\enthought\canopy\user\scripts;C:\Program Files\R\R-3.0.0\bin\x64\;C:\Users\ry\AppData\Local\Enthought\Canopy\User;C:\Users\ry\AppData\Local\Enthought\Canopy\User\Scripts


# In[ ]:
# If we want to run this example in command line, you should comment the before line "simple_process ...." and type the following in the command line:
#dispel4py simple Testing_dispel4py.py -i 5



