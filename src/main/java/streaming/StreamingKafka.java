package streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingKafka {

	public static void main(String[] args) throws InterruptedException {

		// Lancer zookeeper bin/zookeeper-server-start.sh
		// config/zookeeper.properties
		// Lancer kafka bin/kafka-server-start.sh config/server.properties
		// Créer les deux topics bin/kafka-topics.sh --create --zookeeper
		// localhost:2181 --replication-factor 1 --partitions 1 --topic test
		// bin/kafka-topics.sh --create --zookeeper localhost:2181
		// --replication-factor 1 --partitions 1 --topic test1

		// Ecrire dans topic test
		// bin/kafka-console-producer.sh --broker-list localhost:9092 --topic
		// test
		// Lire depuis test1
		// bin/kafka-topics.sh --create --zookeeper localhost:2181
		// --replication-factor 1 --partitions 1 --topic test

		SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext("local[*]", "NetworkWordCount", new Duration(5000)); // 5s
		SparkSession ss = SparkSession.builder().getOrCreate();

		// Pour le moment : On lit depuis le topic test les messages et on les
		// écrit dans le topic test1

		Dataset<Row> ds = ss.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "test").option("startingOffsets", "earliest").load();

		ds.printSchema();

		// Test de modification du ds sur le chemin
		ds.drop("timestamp");

		ds.writeStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "test1")
				.option("checkpointLocation", "testKafka").start();

		// write at the same time in another topic (must exist) :
		// (call start() on both writeStream(), NOT on the single streaming context)
      ds.writeStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "test2")
            .option("checkpointLocation", "testKafka2").start(); // checkpointLocation different than the previous one else error

		// ds.show(); // does not work in streaming

		// ssc.start(); // NOT required
		ssc.awaitTermination();
	}
}
