import yaml
from pyspark.sql.functions import last, split
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def run(spark, config):
    #Load the yaml config file
    with open("config.yaml", "r") as yamlfile:
        config_data = yaml.load(yamlfile, Loader=yaml.FullLoader)
    #Based on is_new_session get the user_session_id
    sessionDF1 = spark.sql("SELECT user_agent, sourceIP, useragenthash, loggedtimestamp, last_event, \
                                          CASE WHEN is_new_session == 1 THEN CONCAT(sourceIP,'_',loggedtimestamp,'_',useragenthash) \
                                               ELSE null END AS user_session_id \
                                   FROM (SELECT *, \
                                                CASE WHEN cast(loggedtimestamp as long) - cast(last_event as long) >= (60 * 15) OR last_event IS NULL \
                                                     THEN 1 ELSE 0 \
                                                END AS is_new_session \
                                         FROM (SELECT *, \
                                                      lag(loggedtimestamp,1) OVER (PARTITION BY sourceIP ORDER BY loggedtimestamp) AS last_event FROM sessionTableIPDF))")

    win = Window.orderBy("sourceIP", "useragenthash", "loggedtimestamp")
    sessionDF = sessionDF1.withColumn("user_session_id", last(F.col("user_session_id"), ignorenulls=True).over(win))
    sessionDF.createOrReplaceTempView("sessionDF")

    #Get number of events, sessionduration, starttimestamp, endtimestamp and other fields
    sessionStatsDF = spark.sql("SELECT user_session_id, min(loggedtimestamp) as starttimestamp, max(loggedtimestamp) as endtimestamp,\
                                cast(max(cast(loggedtimestamp as double)) - min(cast(loggedtimestamp as double)) AS double) AS session_duration, user_agent,\
                                count(*) AS noOfEvents \
                                          FROM sessionDF GROUP BY user_session_id,user_agent")

    split_col = split(sessionStatsDF['user_session_id'], '_')
    sessionStatsDFFinal = sessionStatsDF.select("user_session_id", split_col.getItem(0).alias("IP"),
                          "user_agent","starttimestamp","endtimestamp","session_duration","noOfEvents")
    sessionStatsDFFinal.createOrReplaceTempView("sessionStatsDFFinal")

    sessionStatsDFFinal.write.parquet(config_data[1]['FilePaths']['sessionstatsoutput'])
    sessionStatsDFFinal.limit(100).write.format("csv").option('header',True).save(config_data[1]['FilePaths']['sessionstatsoutputcsv'])
