package streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingKafka {

	public static void main(String[] args) throws InterruptedException {

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

		// Je n'ai pas réussi à faire en sorte d'écrire ds dans deux files
		// différentes en appelant deux fois le writestream.

		// Je n'ai pas réussi à faire en sorte d'afficher le contenu du
		// dataframe dans la console le "ds.show();" il en veut pas

		// ds.show();

		// ssc.start();
		ssc.awaitTermination();
	}
}
