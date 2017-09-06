import pytest
import stats

@pytest.fixture(scope="session")
def spark_context(r):
    spark_conf = SparkConf().setAppName(“pytest”)
    sc = SparkContext(conf=spark_conf)
    r.addfinalizer(lambda: sc.stop())
return sc

def all_nodes():
    spark_conf = SparkConf().setAppName(“pytest”)
    sc = SparkContext(conf=spark_conf)
	result1 = get_number_of_elements("node", sc)
    
    assert len(result1) > 0

def all_ways():
	spark_conf = SparkConf().setAppName(“pytest”)
    sc = SparkContext(conf=spark_conf)
	result2 = get_number_of_elements("way", sc)
	assert len(result2) > 0

def all_relations():
	spark_conf = SparkConf().setAppName(“pytest”)
    sc = SparkContext(conf=spark_conf)
	result3 = get_number_of_elements("relation",sc)
	assert len(result3) > 0

def average_tags_node():
	spark_conf = SparkConf().setAppName(“pytest”)
    sc = SparkContext(conf=spark_conf)
	result1 = get_number_of_tags("node", sc)
    
    assert len(result1) > 0

def average_tags_way():
	spark_conf = SparkConf().setAppName(“pytest”)
    sc = SparkContext(conf=spark_conf)
	result1 = get_number_of_tags("way", sc)
    
    assert len(result1) > 0

def average_tags_relation():
	spark_conf = SparkConf().setAppName(“pytest”)
    sc = SparkContext(conf=spark_conf)
	result1 = get_number_of_tags("relation", sc)
    
    assert len(result1) > 0