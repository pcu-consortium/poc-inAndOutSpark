package inputOutput;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

}
