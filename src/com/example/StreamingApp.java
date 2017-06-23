package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


public class StreamingApp {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf()
				.setAppName("Cassandra Spark Connection")
				.setIfMissing("spark.master", "local[*]")
				.setIfMissing("spark.cassandra.connection.host", "127.0.0.1");
		
		//SparkSession session = SparkSession.builder().config(conf).getOrCreate();
		
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(4));
		jsc.socketTextStream("localhost", 9999).print();
		
		jsc.start();
		jsc.awaitTermination();
		
		

		
	}
}
