import argparse
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import yaml
import importlib
if __name__ == '__main__':

    with open("src/config.yaml", "r") as yamlfile:
        config_data = yaml.load(yamlfile, Loader=yaml.FullLoader)
    conf = SparkConf().setMaster("local[*]").setAppName(config_data[0]['sparkconfig']['appname'])
    conf.set("spark.sql.session.timeZone", config_data[0]['sparkconfig']['sessiontimezone'])
    conf.set("spark.sql.shuffle.partitions", int(config_data[0]['sparkconfig']['shufflepartitions']))

    sc = SparkContext.getOrCreate(conf)
    spark = SparkSession(sc)
    job_module = importlib.import_module('src.tables.TableGenerator')
    job_module1 = importlib.import_module('src.tables.TableGenerator1')
    job_module2 = importlib.import_module('src.tables.TableGenerator2')
    res = job_module.run(spark, 'src.tables.TableGenerator')
    res1 = job_module1.run(spark, 'src.tables.TableGenerator1')
    res2 = job_module2.run(spark, 'src.tables.TableGenerator2')
