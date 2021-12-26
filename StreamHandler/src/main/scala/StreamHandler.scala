import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._

import com.datastax.oss.driver.api.core.uuid.Uuids
import com.datastax.spark.connector._

object StreamHandler {
  def main(args: Array[String]) {

    /** Initialize Spark and Connect to Cassandra Server
     */
    val spark = SparkSession
      .builder
      .appName("Stream Handler")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    import spark.implicits._

    /** Formalize data coming from kafka
     *
     * Creating the Kafka source for streaming queries
     * (Read from Kafka)
     */
    val inputDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "weather")
      .load()

    val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String] // Select only 'value' from the table and convert from bytes to string

    // Split each row on comma and load it to the case class
    val expendedDF = rawDF.map(row => row.split(","))
      .map(row => DeviceData(
        row(1),
        row(2).toDouble,
        row(3).toDouble,
        row(4).toDouble
      ))

    /** Final format of data that will be loaded to Cassandra
     */
    // GroupBy and Aggregate
    val summaryDF = expendedDF
      .groupBy("device")
      .agg(avg("temp"), avg("humd"), avg("pres"))

    // Dataset function that creates UUIDs
    val makeUUID = udf(() => Uuids.timeBased().toString)

    // Add the UUIDs and renamed the columns
    // This is necessary so that the dataframe matches the table schema in Cassandra
    val summaryWithIDs = summaryDF.withColumn("uuid", makeUUID())
      .withColumnRenamed("avg(temp)", "temp")
      .withColumnRenamed("avg(humd)", "humd")
      .withColumnRenamed("avg(pres)", "pres")

    /** Write dataframe to Cassandra
     */
    /*val query = summaryWithIDs
        .writeStream
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .outputMode("append")
        .format("console")
        .start()
    */
    val query = summaryWithIDs
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
        println(s"Writing to Cassandra $batchID")
        batchDF.write
          .cassandraFormat("weather", "stuff") // table, keyspace
          .mode("append")
          .save()
      }
      .outputMode("append")
      .start()

    // Writing until Ctrl+C
    query.awaitTermination()
  }
}