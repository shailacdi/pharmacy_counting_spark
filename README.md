# Table of Contents
1. [Problem](README.md#problem)
2. [Solution](README.md#solution)
3. [Design](README.md#design)
4. [Assumptions and Dependencies](README.md#assumptions)

# Problem

The dataset is obtained from the Centers for Medicare & Medicaid Services and cleansed to suit the project needs. 
It provides information on prescription drugs prescribed by individual physicians and other health care providers. 
The dataset identifies prescribers by their ID, last name, and first name. It also describes the specific prescriptions 
that were dispensed at their direction, listed by drug name and the cost of the medication

<br>Format of the data file:
<br><t>   	id,prescriber_last_name,prescriber_first_name,drug_name,drug_cost

The above input file needs to be analyzed and processed to produce the following results:
 1. the number of unique prescribers who prescribed the drug. 
 2. total cost of the drug across all prescribers

Format of the output file: Uncompressed parquet. 
Sorted by total drug cost (descending), in case of a tie, sorted by drug_name
<br><t>  Output fields : drug_name,  num_prescriber, total_cost

# Solution 

Several alternatives were tried out and the solution was implemented using a cluster computing framework - Spark/HDFS
Programming Language chosen is Python. A series of transformations and actions on the Resilient Distributed Dataset yielded the 
desired output.
Files required:
1. A python script to upload input file into HDFS, and then invoke the spark driver program
2. A spark driver program (in python) to analyze the input data file and produce results

The following command is initiated to accomplish the above.
<br>
python ./src/pharma_env.py input_data_file HDFS_input_data_folder HDFS_output_data_folder ./src/pharma_counting_spark.py

# Design

 The following are the high level steps to accomplish the results: 
<br>   1. Load the input data file into HDFS using 'hdfs fs put'.
<br>   2. Run a series of transformations and actions as follows
<ul> 		a. filter : strip off the header </ul>
<ul>	  b. map : process line by line retrieving relevant fields - (drug_name, ("last_name,first_name", drug_cost)) </ul>
<ul>  	c. aggregateByKey : consolidate drug_costs and maintain a set of unique prescribers for each drug </ul>
<ul>		  d. map : to enable sorting  - (k,v) = ((-drug_cost, drug_name), (drug_name, size of the prescribers set))</ul>
<ul>		  e. sortByKey : sort by drug_cost desc, and drug_name asc</ul>
<ul>		  f. map : discard the key, and retain just the value (drug_name, size of the prescribers set)</ul>
<br>	  3. Generate output file
<ul>  		a. convert RDD to a DataFrame with the schema ["drug_name","num_prescriber","total_cost"] </ul>
<ul>		  b. set to "uncompressed" mode using sqlContext</ul>
<ul>		  c. save the DF as a parquet file</ul>


# Assumptions and Dependencies
1. The following packages of the Standard library have been used
    <br>pyspark
    <br>sys
    <br>re
2. The input file is cleansed and validated as a pre-requisite. Individual field validations aren't necessary.
    
