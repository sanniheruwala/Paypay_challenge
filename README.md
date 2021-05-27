##  To do

#### Part A) Write a clean pyspark program which
```
  main.py          :  Command Line Input        : main.py   with  config.yaml
  spark_submit.sh :  Spark Submit bash script   + different arguments on the node size....
  
  preprocess_session.py
  preprocess_features.py
 
 

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times


Intermediate tables on disk /parquet files on DISK
with partition


   Need to create 2 GLOBAL unique id :
           user_id          : Track user unique identifier.

          user_session_id  :  track session of user.
        

    user_id = ip + user_agent  (+ cookie_XXX)   ( == login user in windows/Mac/IOS ....)
    
    user_session_id =   user_id + TimeStampStart   (bots can have muttiple events per user_id.... at same miliseconds timestamps: multi-thread bots)
    



session_table  ==  Structure version of original table.

###  user_session_id,  ip, dt, date, day, hour, min,  .... other fields

   partition by (date, hour)
   dt : original timestamp
   date :  2021-05-01  (string)
   user_session_id  == ip+user_agent_hash+Timestamp
 

### ip_stats_session_table
  ip, date,  ip_prefix, ip_hash, n_events, n_session, session_time_avg, session_nunique_url, session_time_max, session_time_min,
      user_agent, device, os, browser, ...

   partition by (ip_hash)

   ip_hash   :  HASH(ip) into 500 buckets.
   ip_prefix :  AA.BB.CC    only top 3 components i


### user_session_stats table

user_session_id, start_dt, end_dt, duration, n_events





time_stats_agg

  date, hour, min,  n_unqiue_ip, n_events,   n_events_per_ip,


```


#### part B) Using Spark MLLib,
```
5. Predict the expected load (requests/second) in the next minute

6. Predict the session length for a given IP

7. Predict the number of unique URL visits by a given IP
```


##### Infos
- For this dataset, complete the sessionization by time window rather than navigation. 

Feel free to determine the best session window time on your own, or start with 15 minutes.


log file format is

http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format






