# SF Crime statistics

## Development and testing workflow (Windows):

Run Zookeeper and Kafka in separate consoles:
>.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties 
>.\bin\windows\kafka-server-start.bat .\config\server.properties 

Once Kafka is up and running, activate the producer by running the following command on a separate console:
>python3 kafka_server.py

This will start producing the events stored on the police-department-calls-for-service.json and have them ready for spark to use.
Run the follwoing command:

>spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --master local[*] data_stream.py

In my case I had to run an updated version of the spark-sql-kafka maven.

## Screenshots 
### Consumer test
![Consumer test](https://github.com/johan855/sf_crime_statistics/blob/master/screenshots/consumer_test.png?raw=true "Consumer test")


### Q1 Aggregation
![Q1 Aggregation](https://github.com/johan855/sf_crime_statistics/blob/master/screenshots/Q1.png?raw=true "Q1 Aggregation")


### Q1 Join
![Q1 Join](https://github.com/johan855/sf_crime_statistics/blob/master/screenshots/Q2.png?raw=true "Q1 Join")


### Streaming UI
![Streaming UI](https://github.com/johan855/sf_crime_statistics/blob/master/screenshots/Streaming_UI.png?raw=true "Streaming UI")



How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
> Due to the size of the dataset, changing the session's parameters didn't have much effect.
> Switching the maxOffsetPerTrigger to lower or higher values did not provide any important improvement or delay for the data.
> I would compared the results of 2 sets of values on on the maxOffsetsPerTrigger property and check how the general performance is on the Streaming Query Statistics
> output.
> For this size of data, the higher the value the lower the latency.

What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
> spark.sql.shuffle.partitions: 100
> spark.default.parallelism: 20
> spark.executor.memory: 13g  (more or less 80% of my total RAM and not on session but on the Spark configuration)
> I noticed that after testing a few possibilities, due to the size of my working CPU, the memory property had the highest impact.
> The key metric to determine if something is working better or not is "processedRowsPerSecond" on the progress reporter
> and the performance of the /StreamingQuery/statistics reports on the UI. (Keeping in mind that the main thing to improve performance is first to check that the code/queries are optimized)

Reference sources: 
[Spark documentation](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
[AWS blog](https://aws.amazon.com/fr/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr)



