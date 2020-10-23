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
![Consumer test](screenshots/consumer_test.png?raw=true "Consumer test")


### Q1 Aggregation
![Q1 Aggregation](screenshots/Q1.png?raw=true "Q1 Aggregation")


### Q1 Join
![Q1 Join](screenshots/Q2.png?raw=true "Q1 Join")


### Streaming UI
![Streaming UI](screenshots/Streaming UI.png?raw=true "Streaming UI")