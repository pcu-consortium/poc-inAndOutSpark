package inputOutput;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.elasticsearch.spark.sql.EsSparkSQL;

/**
 * Class used to write data
 * 
 * @author Thomas Estrabaud - Smile
 *
 */
public class WriteOutput {

	/**
	 * Write json data on the file system
	 * 
	 * @param entrees
	 * @param path
	 *            path to write to
	 */
	public static void printToFs(Dataset<Row> entrees, String path) {
		entrees.write().json(path);
	}

	/**
	 * Write the data into Elasticsearch
	 * 
	 * @param entrees
	 *            data to write
	 * @param index
	 *            Elasticsearch's index to write to
	 */
	public static void printToES(Dataset<Row> entrees, String index) {

		EsSparkSQL.saveToEs(entrees.select("*"), index);
	}

	public static void printToKafka(Dataset<Row> entrees, String topic, String key) {

		Dataset<Row> df = entrees.toJSON().withColumn("key", functions.lit(key));

		df.show(50, false);
		df.printSchema();

		df.selectExpr("key", "value", "value", "key").write().format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092").option("topic", topic).save();

	}

	public static void printParquetFile(Dataset<Row> entrees, String path) {

		entrees.write().parquet("people.parquet");
	}

}
