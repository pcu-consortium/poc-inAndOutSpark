package inputOutput;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.spark.sql.EsSparkSQL;

public class WriteOutput {

	public static void printToFs(Dataset<Row> entrees, String path) {
		entrees.write().json(path);
	}

	public static void printToES(Dataset<Row> entrees, String index) {

		EsSparkSQL.saveToEs(entrees.select("*"), index);
	}

}
