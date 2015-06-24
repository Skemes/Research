
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
from dispel4py.new.simple_process import process as simple_process
import time
import sys

class TupleProducer(ProducerPE):
	def __init__(self, inputFile, numRepeats):
		ProducerPE.__init__(self)
		self.file = inputFile
		self.reps = numRepeats
        
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

class CollectCounts(GenericPE):

	def __init__(self, outputFileName):
		GenericPE.__init__(self)
		self._add_input("input", grouping=[0])
		self.outFile = outputFileName
		self.myDict = defaultdict(int)

	def _process(self, tupl):
		#self.log("looking at tuple - %s " % tupl)
		t = tupl.values()[0]
		if t in self.myDict:
			self.myDict[t] += 1
		else:
			self.myDict[t] = 1

	def _postprocess(self):
		writeFH = open(self.outFile, 'w')
		for k, v in self.myDict.iteritems():
			writeFH.write(k + " " + str(v) + "\n")


# Running the workflow in the notebook by using a handy function called "simple_process":
# This function allows us to run a graph, by indicanting the first PE, and the number of iterations that we want to run it. e.g. 
#  * simple_process(graph, {producer: 1}) 
# 
# It runs 1 iteration of the graph
# 

# In[10]:

def timer(timeList):
	timeList.append(time.time())
	if len(timeList)%2 == 0:
		print 'Time elapsed: ' + str(round(timeList[-1] - timeList[-2],4)) + ' seconds.'
		timeList.pop()
		timeList.pop()


def main():
	if len(sys.argv) < 4:
		print "Incorrect arguments provided. Proper format: python tupleCounter.py <inputFile> <numRepeats> <outputFile>"
		sys.exit()
	inputFilename = sys.argv[1]
	numRepeats = int(sys.argv[2])
	outputFile = sys.argv[3]


	producer = TupleProducer(inputFilename, numRepeats)
	collector = CollectCounts(outputFile)

	graph = WorkflowGraph()
	graph.connect(producer, 'output', collector, 'input')
	simple_process(graph, {producer: 1})


if __name__ == '__main__':
	timeList = []
	timer(timeList)
	main()
	timer(timeList)

# In[ ]:
# If we want to run this example in command line, you should comment the before line "simple_process ...." and type the following in the command line:
#dispel4py simple Testing_dispel4py.py -i 5



