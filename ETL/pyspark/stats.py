#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession

def main(argv):
	spark_conf = SparkConf().setAppName(“Stats”)
	sc = SparkContext(conf = spark_conf) 

	get_number_of_elements("node", sc)
    get_number_of_elements("way", sc)
    get_number_of_elements("relation",sc)
    get_number_of_tags("node", sc)
    get_number_of_tags("way", sc)
    get_number_of_tags("relation",sc)
    get_number_of_routes(sc)
    get_number_of_users("node", sc)
    get_number_of_users("way", sc)
    get_number_of_users("relation",sc)

def get_number_of_elements(type="node", sc):
	if type == "node":
		file = sc.textFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/osmnodes")
		result = file.flatMap(lambda line: line.split(",")).map(lambda element: (element, 1)).reduceByKey(lambda a, b: a + b)
		result.saveAsTextFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/stats")
	elif type == "way":
		file = sc.textFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/osmways")
		result = file.flatMap(lambda line: line.split(",")).map(lambda element: (element, 1)).reduceByKey(lambda a, b: a + b)
		result.saveAsTextFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/stats")
	elif type == "relation":
		file = sc.textFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/osmrelations")
		result = file.flatMap(lambda line: line.split(",")).map(lambda element: (element, 1)).reduceByKey(lambda a, b: a + b)
		result.saveAsTextFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/stats")

	return result

def get_number_of_tags(type="node", sc):
		if type == "node":
		file = sc.textFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/osmnodes")
		result = file.flatMap(lambda line: line.split(",")).map(lambda element: x[5].split(",")).map(lambda element: (element,1)).reduceByKey(lambda a, b: a + b)
		result.saveAsTextFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/stats")
	elif type == "way":
		file = sc.textFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/osmways")
		result = file.flatMap(lambda line: line.split(",")).map(lambda element: x[5].split(",")).map(lambda element: (element,1)).reduceByKey(lambda a, b: a + b)
		result.saveAsTextFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/stats")
	elif type == "relation":
		file = sc.textFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/osmrelations")
		result = file.flatMap(lambda line: line.split(",")).map(lambda element: x[5].split(",")).map(lambda element: (element,1)).reduceByKey(lambda a, b: a + b)
		result.saveAsTextFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/stats")

	return result

def get_number_of_routes(sc):
		file = sc.textFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/osmways")
		result = file.flatMap(lambda line: line.split(",")).map(lambda element: x[5] == 'route').map(lambda element: (element,1)).reduceByKey(lambda a, b: a + b)
		result.saveAsTextFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/stats")

		return result

def get_number_of_users(type="node", sc):
		if type == "node":
		file = sc.textFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/osmnodes")
		result = file.flatMap(lambda line: line.split(",")).map(lambda element: x[1].split(",")).map(lambda element: (element,1)).reduceByKey(lambda a, b: a + b)
		result.saveAsTextFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/stats")
	elif type == "way":
		file = sc.textFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/osmways")
		result = file.flatMap(lambda line: line.split(",")).map(lambda element: x[1].split(",")).map(lambda element: (element,1)).reduceByKey(lambda a, b: a + b)
		result.saveAsTextFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/stats")
	elif type == "relation":
		file = sc.textFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/osmrelations")
		result = file.flatMap(lambda line: line.split(",")).map(lambda element: x[1].split(",")).map(lambda element: (element,1)).reduceByKey(lambda a, b: a + b)
		result.saveAsTextFile("hdfs://udltest3.cs.ucl.ac.uk:8020/users/hive/warehouse/stats")

	return result



if __name__ == "__main__":
    main(sys.argv[1:])