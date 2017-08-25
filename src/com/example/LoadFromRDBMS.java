package com.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class LoadFromRDBMS {
	private static SparkSession spark = null;
	
	public static void registerTable(String serverName, String dbName, String table, String username, String password) {
		spark
			.read()
			.format("jdbc")
			.option("url", "jdbc:mysql://" + serverName + "/" + dbName)
			.option("driver", "com.mysql.jdbc.Driver")
			.option("dbtable", table)
			.option("user", username)
			.option("password", password)
			.load()
			.createOrReplaceTempView(table);
	}
	
	
	public static void main(String[] args) {
		
		String keyspace = "demo";
		String table = "employees";
		
		SparkConf conf = new SparkConf()
				.setAppName(LoadToCassandra.class.getName())
				.setIfMissing("spark.master", "local[*]");
		
		spark = SparkSession.builder().config(conf).getOrCreate();
		registerTable("localhost", "employees", "employees", "root", "training");
		
		spark.sql("select * from employees").show();
		
		/*
		spark.table("employees")
		.write()
		.format("csv")
		.mode(SaveMode.Overwrite)
		.save("employees-csv");
		*/

		Map<String, String> options = new HashMap<>();
		options.put("table", table);
		options.put("keyspace", keyspace);
		
		spark.table("employees")
		.write()
		.format("org.apache.spark.sql.cassandra")
		.options(options)
		.mode(SaveMode.Append)
		.save();
	}
}
