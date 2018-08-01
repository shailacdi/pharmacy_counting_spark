#pharmacy_counting_spark.py

"""
This spark program processes a data file related to prescribed drugs and sales, and
generates a report with details of prescribed drug along with unique number of prescribers 
and the overall cost for the same.

Command line arguments : 
	1. input folder in HDFS (csv file - valid fields, cleansed)
	2. output folder in HDFS (uncompressed parquet file)

Format of the input file:
id,prescriber_last_name,prescriber_first_name,drug_name,drug_cost

Format of the output file: Uncompressed parquet. Sorted by total drug cost (descending), in case of a tie, sorted by drug_name
Output fields - drug_name,  num_prescriber, total_cost
"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys
import re

# Spark configurations - can be read from a config file as a future enhancement
CONFIG_MASTER = "yarn-client"
CONFIG_APP = "pharmacy"

# CSV field positions
LAST_NAME = 1
FIRST_NAME = 2
DRUG_NAME = 3
DRUG_COST = 4


def split_fields(x, regex):
	"""
	split fields and return (drug_name, ("last_name,first_name", drug_cost ))
	Input fields:
		x : record of format 
		regex : regular expression object with a preset pattern to handle "," in name fields
	"""
	fields = regex.split(x)
	return (fields[DRUG_NAME], (fields[LAST_NAME]+','+ fields[FIRST_NAME], float(fields[DRUG_COST])))


def seq_func_aggregate_values(agg, rec):
	"""
	This is a reducer aggregation function for every key handle.
	Input paramenters:
		agg : aggregated values - (float, set[]) ie. (aggregated drug_cost, set of unique prescribers)
		rec : corresponds to individual input record -m(string, float) ie. (prescriber name, individual drug_cost)
	Output :
		reducer results - (float, set[]) ie. (aggregated drug_cost, updated set of unique prescribers)	
	"""
	# the set will thus maintain unique prescribers for each drug
	agg[1].add(rec[0])
    # return the updated set of prescribers and the aggregated drug costs
	return (agg[0]+rec[1], agg[1])


def comb_func_aggregate_values(agg1, agg2):
	"""
	This is a combiner aggregation function for every key handle.
	Input paramenters:
		agg1 : corresponds to a reducer output - (float, set[]) ie. (aggregated drug_cost, set of unique prescribers)
		agg2 : corresponds to another reducer output - (float, set[]) ie. (aggregated drug_cost, set of unique prescribers)
	Output :
		combiner results - (float, set[]) ie. (aggregated drug_cost, combined set of unique prescribers)	
	"""
	# return the union of sets of prescribers and the aggregated drug costs
	return (agg1[0]+agg2[0], agg1[1].union(agg2[1]))

def	process_inputfile(sc, hdfs_input_folder):
	"""
	This function loads, transforms and aggregates output values
	Input paramenters
		sc - SparkContext
		hdfs_input_folder - location of the input data file in HDFS
	Output
		dataAggrSortedMap - map that contains sorted data in the format (drug_name, number of unique prescribers, drug cost)   
	"""
	# Load the input data file
	dataRaw = sc.textFile(hdfs_input_folder)

	# filter the header
	header = dataRaw.first()
	dataFilter = dataRaw.filter(lambda x : x!=header)

	# names may have a comma and might embed the field in ""
	regex = re.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")


	# split, retrieve the fields and map in the  following format (k,v) = (drug_name, ("last_name,first_name", drug_cost ))
	dataMap = dataFilter.map(lambda rec : split_fields(rec, regex))

	#aggregateByKey to add drug_costs and maintain a set of unique prescribers for each drug 
	dataAggr = dataMap.aggregateByKey((0.0,set([])), lambda agg,rec : seq_func_aggregate_values(agg,rec), lambda agg1,agg2 : comb_func_aggregate_values(agg1,agg2))

	#map  to enable sorting (k,v) = ((-drug_cost, drug_name), (drug_name, size of the prescribers set)
	#sort by key which is drug cost desc and drug name ascending
	#map to have the format (drug_name, num of unique prescribers, drug_cost)
	dataAggrSortedMap = dataAggr.map(lambda x : ((-x[1][0],x[0]), (x[0], len(x[1][1]), round(x[1][0],2)))). \
					sortByKey(). \
					map(lambda x : x[1])
	# return a ready-to-write output data					
	return dataAggrSortedMap


def write_outputfile(pharmaOutputMap,hdfs_output_folder):
	"""
	This function writes the output in a uncompressed parquet format
	Input parameters
		pharmaOutputMap - RDD with sorted output data
		hdfs_output_folder - folder where the output file needs to be created in HDFS	
	"""
	# convert to dataframe
	pharmaOutputDF = sqlContext.createDataFrame(pharmaOutputMap, ["drug_name","num_prescriber","total_cost"])

	# set config so that compression is turned off
	sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")
	# write a single output parquet file
	pharmaOutputDF.coalesce(1).write.parquet(hdfs_output_folder)


if __name__ == '__main__':
	# check if command line arguments are provided
	if (len(sys.argv) != 3):
		print 'Invalid inputs. Please provide valid arguments..Try again..'
		sys.exit()

	# retrieve HDFS input data folder name
	hdfs_input_folder = sys.argv[1]
	# retrieve HDFS output data folder name
	hdfs_output_folder = sys.argv[2]

	# Inititalize  SparkConfig, SparkContext and sqlContext
	conf = SparkConf().setMaster(CONFIG_MASTER).setAppName(CONFIG_APP)
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

	#load, transform and aggregate the required fields 
	pharmaOutputMap = process_inputfile(sc, hdfs_input_folder)

	#write output in the desired format
	write_outputfile(pharmaOutputMap,hdfs_output_folder)
