#pharma_env.py

"""
This python program uploads a local data input file into HDFS, and invokes the 
Spark program to process it further

Command line arguments : 
	1. Input filename (csv file)
	2. HDFS input folder name 
	3. HDFS output folder name
	4. Spark processing program file name

"""

import os
import sys

# Check if all the command line parameters are provided
if (len(sys.argv) != 5):
		print 'Invalid inputs. Please provide input, hdfs, output and program file names..Try again..'
		sys.exit()

# Retrieve local input data file name from command line arguments
input_file = sys.argv[1]
# Retrieve HDFS input folder name from command line arguments
hdfs_input_folder = sys.argv[2]	
# Retrieve HDFS output folder name from command line arguments
hdfs_output_folder = sys.argv[3]
# Retrieve program file name from command line arguments
program_file = sys.argv[4]

# Check if the local input data file exists
if not os.path.exists(input_file):
	print 'Input data file does not exist. Please provide a valid file..Try again..'
	sys.exit()

os.system('echo Uploading the input data file to HDFS...')
command = 'hadoop fs -put %s %s' %(input_file,hdfs_input_folder)
print 'Executing command,', command 
# Execute hadoop "put" command to upload the local data file into HDFS
os.system(command)


command = 'spark-submit %s %s %s' %(program_file,hdfs_input_folder,hdfs_output_folder)
print 'Executing command, ', command
# Execute the spark program by invoking spark-submit
os.system(command)
