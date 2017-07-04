package inputOutput;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class used to read data from somewhere
 * 
 * @author Thomas Estrabaud - Smile
 *
 */
public class ReadInput {

	/**
	 * Read json data from a file/folder
	 * 
	 * @param ss
	 * @param path
	 *            The path of the file/folder
	 * @return
	 */
	public static Dataset<Row> readJSONFromFile(SparkSession ss, String path) {

		return ss.read().json(path);
	}

	/**
	 * Read data from a kafka topic
	 * 
	 * @param ss
	 * @param topic
	 *            Topic to read from
	 * @param IpBrokers
	 *            Ip of the brokers we read from
	 * @return
	 */
	public static Dataset<Row> readJSONFromKafka(SparkSession ss, String topic, String IpBrokers) {

		Dataset<Row> ds1 = ss.read().format("kafka").option("kafka.bootstrap.servers", IpBrokers)
				.option("subscribe", topic).load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic",
						"partition", "offset", "timestamp", "timestampType");

		JavaRDD<String> jrdd2 = ds1.select(ds1.col("value")).toJavaRDD().map(v1 -> v1.mkString());

		return ss.read().json(jrdd2);
	}

}
