package inputOutput;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import com.esotericsoftware.minlog.Log;

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

		Dataset<Row> ds = ss.read().format("kafka").option("kafka.bootstrap.servers", IpBrokers)
				.option("subscribe", topic).load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic",
						"partition", "offset", "timestamp", "timestampType");
		ds.show();
		Log.warn(topic + " " + IpBrokers);
		JavaRDD<String> jrdd = ds.select(ds.col("value")).toJavaRDD().map(v1 -> v1.mkString());

		return ss.read().json(jrdd);
	}

	public static Dataset<Row> readDataFromElastic(SparkSession ss, String index, String request) {

		return JavaEsSparkSQL.esDF(ss, index, request);

	}

}
