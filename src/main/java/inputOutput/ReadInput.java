package inputOutput;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadInput {

	public static Dataset<Row> readJSONFromFile(SparkSession ss, String path) {

		return ss.read().json(path);
	}

	public static Dataset<Row> readJSONFromKafka(SparkSession ss, String topic, String IpBrokers) {

		Dataset<Row> ds1 = ss.read().format("kafka").option("kafka.bootstrap.servers", IpBrokers)
				.option("subscribe", topic).load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic",
						"partition", "offset", "timestamp", "timestampType");

		JavaRDD<String> jrdd2 = ds1.select(ds1.col("value")).toJavaRDD().map(v1 -> v1.mkString());

		return ss.read().json(jrdd2);
	}

}
