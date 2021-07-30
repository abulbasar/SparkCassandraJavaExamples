package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class SparkCassandraApp {

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

	public static void main(String[] args) {


		String dataDir = "/home/cassandra/Downloads/ml-latest-small";

		SparkConf conf = new SparkConf()
				.setAppName(SparkCassandraApp.class.getName())
				.setIfMissing("spark.master", "local[*]")
				.setIfMissing("connection.port", "9042")
				.setIfMissing("spark.cassandra.connection.host", "demo0")
				.setIfMissing("spark.cassandra.auth.username", "cassandra")
				.setIfMissing("spark.cassandra.auth.password", "cassandra")
				;

		spark = SparkSession.builder().config(conf).getOrCreate();

		String moviesPath = dataDir + "/movies.csv";
		String ratingsPath = dataDir + "/ratings.csv";

		Dataset<Row> movies = createSparkView(moviesPath, "movies");
		Dataset<Row> ratings = createSparkView(ratingsPath, "ratings");

		movies.show();
		ratings.show();


		movies
				.withColumnRenamed("movieId", "movieid")
				.write()
				.format("org.apache.spark.sql.cassandra")
				.option("keyspace", "demo")
				.option("table", "movies")
				.mode(SaveMode.Append).save();


		ratings
				.withColumnRenamed("movieId", "movieid")
				.withColumnRenamed("userId", "userid")
				.write()
				.format("org.apache.spark.sql.cassandra")
				.option("keyspace", "demo")
				.option("table", "ratings")
				.mode(SaveMode.Append).save();


		/*Dataset<Row> moviesAgg = spark.sql("select t1.movieId movieid, t1.title, avg(t2.rating) avg_rating from "
				+ " movies t1 join ratings t2 on t1.movieId = t2.movieId group by "
				+ " t1.movieId, t1.title order by avg_rating desc");
		moviesAgg.show();*/



		// Let's create a direct Cassandra session to create a table in Cassandra
//		CassandraConnector cassandraConnector = CassandraConnector.apply(spark.sparkContext());
//
//		Session cassandraConnection = cassandraConnector.openSession();
//
//		cassandraConnection.execute(
//				"create table if not exists demo.movies_agg(movieid int primary key, title text, avg_rating float)");
//
//		cassandraConnection.close();

		/*moviesAgg
				.write()
				.format("org.apache.spark.sql.cassandra")
				.option("keyspace", "demo")
				.option("table", "movies_agg")
				.mode(SaveMode.Append).save();


		createSparkView("demo", "movies_agg", "movies_agg");

		System.out.println("Showing data from cassandra table");

		spark.sql("select * from movies_agg").show();*/

		spark.close();

	}

}
