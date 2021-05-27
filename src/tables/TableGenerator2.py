import yaml

def run(spark, config):
    #Load the yaml config file
    with open("config.yaml", "r") as yamlfile:
        config_data = yaml.load(yamlfile, Loader=yaml.FullLoader)

    #Get the avg, min and max session duration per IP
    ipMinMaxAvgDF = spark.sql("SELECT IP,avg(session_duration) AS avg_session_duration,max(session_duration) AS max_duration, min(session_duration) AS min_session_duration \
                            FROM sessionStatsDFFinal \
                            GROUP BY IP")

    ipMinMaxAvgDF.write.parquet(config_data[1]['FilePaths']['ipminmaxstatsoutput'])
    ipMinMaxAvgDF.write.format("csv").option('header',True).save(config_data[1]['FilePaths']['ipminmaxstatsoutputcsv'])

    #Get the avg, min and max session duration on a whole. Added dummy column here as spark doesn't allow to get aggregate without GroupBy. Dropping the column immediately after that
    minmaxavgDF = spark.sql("SELECT avg(session_duration) AS avg_session_duration,max(session_duration) AS max_duration,min(session_duration) AS min_duration \
                                FROM (SELECT *, 'dummy' AS dummy \
                                        FROM sessionStatsDFFinal) \
                                GROUP BY dummy").drop('dummy')

    minmaxavgDF.write.parquet(config_data[1]['FilePaths']['minmaxavgoutput'])
    minmaxavgDF.write.format("csv").option('header',True).save(config_data[1]['FilePaths']['minmaxavgoutputcsv'])