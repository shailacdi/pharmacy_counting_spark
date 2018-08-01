#!/bin/bash
# This script runs a python program to initialize the setup required before running the main spark program
python ./src/pharma_env.py ./input/itcont.txt /user/cloudera/pharma_input /user/cloudera/pharma_output ./src/pharma_counting_spark.py
