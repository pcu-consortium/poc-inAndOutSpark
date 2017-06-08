package processors;

import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class Processors {

	public Processors() {

	}

	public Dataset<Row> append(Dataset<Row> df, String commande) {

		String[] str = commande.split(" ");

		String nomColonne = str[1] + "-" + str[2];
		if (str.length >= 4)
			nomColonne = str[3];

		return df.withColumn(nomColonne, functions.concat(df.col(str[1]), df.col(str[2])));

	}

	public Dataset<Row> join(HashMap<String, Dataset<Row>> Hdf, String commande) {

		String[] str = commande.split(" ");

		if (str.length == 4)
			return Hdf.get(str[1]).join(Hdf.get(str[2]), str[3]);
		else if (str.length == 5)
			return Hdf.get(str[1]).join(Hdf.get(str[2]),
					Hdf.get(str[1]).col(str[3]).equalTo(Hdf.get(str[2]).col(str[4])));
		else
			return null;
	}

	public Dataset<Row> stringToDate(Dataset<Row> df, String commande) {

		String[] str = commande.split(" ");

		return df.withColumn(str[2], functions.from_utc_timestamp(df.col(str[1]), "+01:00"));

	}

	public Dataset<Row> split(Dataset<Row> df, String commande) {

		String[] str = commande.split(" ");

		return df.withColumn(str[2], functions.split(df.col(str[1]), str[3]));
	}

	public Dataset<Row> aggregate(Dataset<Row> df, String commande) {
		/*
		 * val sessionUrlsDf = spark.
		 * sql("select session.session_id, url, count(*) as rating from cleanedUrlFbPageviewDf where session.session_id is not null group by session.session_id, url order by session_id, rating desc"
		 * )
		 */
		return null;
	}

}
