package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkExample {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Cassandra Spark Connection")
				.setIfMissing("spark.master", "local[*]")
				.setIfMissing("spark.cassandra.connection.host", "127.0.0.1");
		
		SparkSession session = SparkSession.builder().config(conf).getOrCreate();
		
		Dataset<Row> df = session
		.read()
		.format("org.apache.spark.sql.cassandra")
	    .option("table", "user")
	    .option("keyspace", "ks1")
	    .load();
		
		df.show();
		
		System.out.printf("No of partitions: %d\n", df.rdd().partitions().length);
		
		df.printSchema();
		
		session.close();
		
	}
	

}
