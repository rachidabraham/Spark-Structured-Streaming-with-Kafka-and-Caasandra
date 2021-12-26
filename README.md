### Spark Structured Streaming Example

---

#### Pipe

​	**IoT-Devices --> Kafka --> Spark --> Cassandra**

---

#### Quickstart

1. Start a Kafka server
	
	* create a topic called `weather`
1. Start a Cassandra database
	* create a keyspace called `stuff` (SimpleStrategy, replication=1)
		```
		CREATE KEYSPACE stuff
		WITH replication = {'class': 'SimpleStrategy, 'replication_factor' : 1};
		```
	* create a table called `weather` with the following schema
		```
		CREATE TABLE weather (
			uuid uuid primary key,
			device text,
			temp double,
			humd double,
			pres double
		);
	  ```
1. Inside the `StreamHandler` directory, package up the Scala file:
	```
	sbt package
	```
1. Then run
	``` 
	spark-submit --class StreamHandler \
	--master local[*] \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,\
		com.datastax.cassandra:cassandra-driver-core:4.0.0,\
		com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 \
	target/scala-2.11/stream-handler_2.11-1.0.jar
	```
1. From root directory, start one or more "IoT devices":
	```
	./iot_devices.py boston
	./iot_devices.py denver
	./iot_devices.py losang
	```
1. `select * from weather` from CQLSH to see if the data is being processed saved correctly!



---

**Tuto Instructor** : [David Busteed](https://github.com/dbusteed/spark-structured-streaming)