from pyspark.sql.types import StructType,StringType,TimestampType
from pyspark.sql.functions import udf,split, hour, minute, hash, expr, to_date, unix_timestamp,count,countDistinct, regexp_replace, substring
from src.tables.GetFamiliesFromUserAgent import getall_families_from_useragent
import yaml

def run(spark, config):
    #Load the yaml config file
    with open("config.yaml", "r") as yamlfile:
        config_data = yaml.load(yamlfile, Loader=yaml.FullLoader)

    #UDF to get browser, device and OS info from user agent field
    udfAll3Family = udf(getall_families_from_useragent, StringType())

    #Defining the schema for log file
    logSchema = StructType().add("loggedtimestamp", "string").add("elb", "string").add("client_port", "string").add("backend_port", "string") \
                            .add("request_processing_time", "string").add("backend_processing_time", "string").add("response_processing_time", "string") \
                            .add("elb_status_code", "string").add("backend_status_code", "string").add("received_bytes", "string") \
                            .add("sent_bytes", "string").add("request", "string").add("user_agent", "string") \
                            .add("ssl_cipher", "string").add("ssl_protocol", "string")

    #Read the log file into a dataframe
    elbLogDF = spark.read \
                    .format("com.databricks.spark.csv")   \
                    .option("header", "false") \
                    .option("delimiter"," ") \
                    .schema(logSchema) \
                    .load(config_data[1]['FilePaths']['inputfilepath'])

    #Get the UDF defined for user agent into  a dataframe
    elbLogFinalDF = elbLogDF.withColumn("All3UserAgent",udfAll3Family(elbLogDF.user_agent))

    #Get Ip and port separately
    All3UserAgent = split(elbLogFinalDF['All3UserAgent'], ' ')
    sourceip_port = split(elbLogFinalDF['client_port'], ':')
    backendip_port = split(elbLogFinalDF['backend_port'], ':')
    request = split(elbLogFinalDF['request'], ' ')


    #Select all the required fields to be stored into sessionTable
    sessionTableDF = elbLogFinalDF.select("loggedtimestamp",
                                        to_date("loggedtimestamp").alias("loggeddate"),
                                        hour("loggedtimestamp").alias("hour"),
                                        minute("loggedtimestamp").alias("minute"),
                                        "elb",
                                        sourceip_port.getItem(0).alias("sourceIP"),
                                        sourceip_port.getItem(1).alias("sourcePort"),
                                        backendip_port.getItem(0).alias("backendIP"),
                                        backendip_port.getItem(1).alias("backendPort"),
                                        "request_processing_time",
                                        "backend_processing_time",
                                        "elb_status_code",
                                        "backend_status_code",
                                        "received_bytes",
                                        "sent_bytes",
                                        request.getItem(1).alias("URL"),
                                        "user_agent",
                                        All3UserAgent.getItem(0).alias("os"),
                                        All3UserAgent.getItem(1).alias("device"),
                                        All3UserAgent.getItem(2).alias("browser"))
    #cache the dataframe as it will be used down the line
    sessionTableDF.cache()
    #write to parquet file with a partition on loggeddate and hour
    sessionTableDF.write.partitionBy("loggeddate","hour").parquet(config_data[1]['FilePaths']['sessiontableoutput'])
    sessionTableDF.limit(100).write.format("csv").option('header',True).save(config_data[1]['FilePaths']['sessiontableoutputcsv'])

    #Get the noofUniqueIp,noOfEvents per loggeddate,hour and minute combination
    sesssionAgg = sessionTableDF.groupBy("loggeddate", "hour", "minute").agg(countDistinct("sourceIP").alias("noOfUniqueIP"), count("*").alias("noOfEvents"))
    sesssionAgg.write.parquet(config_data[1]['FilePaths']['sessionaggoutput'])
    sesssionAgg.limit(100).write.format("csv").option('header',True).save(config_data[1]['FilePaths']['sessionaggoutputcsv'])

    #Get the number of events per IP
    sessionIpAgg = sessionTableDF.groupBy("sourceIP").agg(count("*").alias("noOfEventsPerIP"))
    #Add hash and determine the bucketnumber before writing to parquet
    sessionTableHashDF = sessionIpAgg.withColumn("IPPrefix",regexp_replace('sourceIP', '\\.\\d+$', '')) \
                                     .withColumn("IPHash",hash("sourceIP")) \
                                     .withColumn("IPHashBucket",expr("mod(abs(hash(sourceIP)),500)"))
    #write to parquet file with a partition on IPHash
    sessionTableHashDF.write.partitionBy("IPHash").parquet(config_data[1]['FilePaths']['sessiontablehashoutput'])
    sessionTableHashDF.limit(100).write.format("csv").option('header',True).save(config_data[1]['FilePaths']['sessiontablehashoutputcsv'])

    #Get the noofRequests per os,device and browser and write to a separate spark file
    sessionTableDF.groupBy("os").agg(count("*").alias("noOfRequests")).write.parquet(config_data[1]['FilePaths']['osaggoutput'])
    sessionTableDF.groupBy("os").agg(count("*").alias("noOfRequests")).write.format("csv").option('header',True).save(config_data[1]['FilePaths']['osaggoutputcsv'])
    sessionTableDF.groupBy("browser").agg(count("*").alias("noOfRequests")).write.parquet(config_data[1]['FilePaths']['browseraggoutput'])
    sessionTableDF.groupBy("browser").agg(count("*").alias("noOfRequests")).write.format("csv").option('header',True).save(config_data[1]['FilePaths']['browseraggoutputcsv'])
    sessionTableDF.groupBy("device").agg(count("*").alias("noOfRequests")).write.parquet(config_data[1]['FilePaths']['deviceaggoutput'])
    sessionTableDF.groupBy("device").agg(count("*").alias("noOfRequests")).write.format("csv").option('header',True).save(config_data[1]['FilePaths']['deviceaggoutputcsv'])

    sessionTableIPDF = sessionTableDF.withColumn("loggedtimestamp",(unix_timestamp("loggedtimestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX") + substring("loggedtimestamp", -7, 6).cast("float")/1000000).cast(TimestampType())) \
                                     .withColumn("useragenthash", hash("user_agent"))
    sessionTableIPDF.createOrReplaceTempView("sessionTableIPDF")

    return sessionTableIPDF
