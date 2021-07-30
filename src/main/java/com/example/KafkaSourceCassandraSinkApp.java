package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/*

Python producer https://raw.githubusercontent.com/abulbasar/pyspark-examples/master/kafka-clients/json_producer.py

Cassandra table definition:

{"id": "1ae8778d-f121-11eb-a156-005056bb9b69",
"merchant_id": "m468",
"customer_id": "c787141",
"amount": 125.48245224179323,
"category": "net",
"timestamp": 1627641006
}


create table if not exists tnx(
	id uuid,
	merchant_id text,
	customer_id text,
	amount double,
	category text,
	timestamp bigint,
	primary key (customer_id, id)
);

*/

public class KafkaSourceCassandraSinkApp {

	private static SparkSession spark = null;

	public static Dataset<Row> createSparkView(String path, String tableName) {
		Dataset<Row> dataset = spark.read().format("csv").option("header", true).option("inferSchema", true).load(path);
		dataset.createOrReplaceTempView(tableName);
		return dataset;
	}

	public static void createSparkView(String keySpace, String tableName, String viewName) {
		spark.read().format("org.apache.spark.sql.cassandra").option("keyspace", keySpace)
				.option("table", tableName).load().createOrReplaceTempView(viewName);

	}

	public static void main(String[] args) throws StreamingQueryException {


		String dataDir = "/home/cassandra/Downloads/ml-latest-small";
		String topicName = "tnx";

		SparkConf conf = new SparkConf()
				.setAppName(KafkaSourceCassandraSinkApp.class.getName())
				.setIfMissing("spark.master", "local[*]")
				.setIfMissing("connection.port", "9042")
				.setIfMissing("spark.cassandra.connection.host", "demo0")
				.setIfMissing("spark.cassandra.auth.username", "cassandra")
				.setIfMissing("spark.cassandra.auth.password", "cassandra")
				.setIfMissing("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
				;

		spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> stream = spark
				.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", topicName)
				.load()

				;

		stream.printSchema();

		Dataset<Row> jsonValues = stream
				.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset")
				.select("value")
				.withColumn("id", functions.get_json_object(functions.col("value"), "$.id"))
				.withColumn("customer_id", functions.get_json_object(functions.col("$.customer_id"), "$.id"))
				.withColumn("merchant_id", functions.get_json_object(functions.col("value"), "$.id"))
				.withColumn("amount", functions.get_json_object(functions.col("value"), "$.id"))
				.withColumn("category", functions.get_json_object(functions.col("value"), "$.id"))
				.withColumn("timestamp", functions.get_json_object(functions.col("value"), "$.id"))
				.withColumn("amount", functions.expr("cast(amount as double)"))
				.withColumn("timestamp", functions.expr("cast(amount as bigint)"))
				.drop("value")
				;

		jsonValues
				.writeStream()
				.format("console")
				.start();

		jsonValues
				.writeStream()
				.format("org.apache.spark.sql.cassandra")
				.option("keyspace", "demo")
				.option("table", "tnx")
				.start();

		spark.streams().awaitAnyTermination();

		spark.close();

	}

}
