package streaming;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class MainStreaming {

	// Tentative de lecture de fichier en local en streaming

	public static void main(String[] args) throws InterruptedException {

		// Initialisation
		SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext("local[*]", "NetworkWordCount", new Duration(5000)); // 5s
		SparkSession ss = SparkSession.builder().getOrCreate();

		// Lecture + affichage en RDD - 1 seule fois
		JavaRDD<String> lines = jssc.sparkContext().textFile("a");
		LogManager logManager = new LogManager();
		Logger log = LogManager.getRootLogger();
		log.warn(lines.collect().toString());

		// Lecture puis affichage en dataframe - 1 seule fois
		Dataset<Row> ds = ss.read().format("json").option("inferSchema", true)
				.load("file:///home/thest/workspace/inAndOutSpark/test/");
		ds.show(false);
		ds.printSchema();

		// Lecture des fichiers dans un dossier (en continu)

		JavaDStream<String> test = jssc.textFileStream("test/");
		test.print();
		jssc.start();
		jssc.awaitTermination();

		// Autre test de lecture en continu d'un dossier (commenter cette partie
		// là ou celle du dessus)
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext("local[*]", "JavaWordCount", new Duration(10000)); // 10s

		JavaDStream<String> textStream = ssc.textFileStream("test/");

		textStream.print();

		ssc.start();
		ssc.awaitTermination();

	}

}
