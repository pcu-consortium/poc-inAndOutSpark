package processors;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import inputOutput.ReadInput;

/**
 * Class containing all the processors natively supported
 * 
 * @author Thomas Estrabaud - Smile
 *
 */
public class Processors implements Serializable {

	/**
	 * Empty constructor used in reflexion
	 */
	public Processors() {

	}

	/**
	 * Append two columns into a third column. The third column can be named
	 * with a parameter or generated with column1-column2
	 * 
	 * @param df
	 * @param commande
	 * @param log
	 * @return
	 */
	public Dataset<Row> append(Dataset<Row> df, String commande, Logger log) {

		String[] str = commande.split(" ");

		String nomColonne = str[1] + "-" + str[2];
		if (str.length >= 4)
			nomColonne = str[3];

		return df.withColumn(nomColonne, functions.concat(df.col(str[1]), df.col(str[2])));

	}

	/**
	 * Do a SQL join on two tables. If the columns have the same name, only
	 * specify it once otherwise, write the 2 names
	 * 
	 * @param Hdf
	 * @param commande
	 * @param log
	 * @return
	 */
	public Dataset<Row> join(HashMap<String, Dataset<Row>> Hdf, String commande, Logger log) {

		String[] str = commande.split(" ");

		if (str.length == 5)
			return Hdf.get(str[2]).join(Hdf.get(str[3]), str[4]);
		else if (str.length == 6)
			return Hdf.get(str[2]).join(Hdf.get(str[3]),
					Hdf.get(str[2]).col(str[4]).equalTo(Hdf.get(str[3]).col(str[5])));
		else
			return null;
	}

	/**
	 * Transform a string representation of a date to a date
	 * 
	 * @param df
	 * @param commande
	 * @param log
	 * @return
	 */
	public Dataset<Row> stringToDate(Dataset<Row> df, String commande, Logger log) {

		String[] str = commande.split(" ");

		return df.withColumn(str[2], functions.from_utc_timestamp(df.col(str[1]), "+01:00"));

	}

	/**
	 * Split the values of a column with a certain separator
	 * 
	 * @param df
	 * @param commande
	 * @param log
	 * @return
	 */
	public Dataset<Row> split(Dataset<Row> df, String commande, Logger log) {

		String[] str = commande.split(" ");

		return df.withColumn(str[2], functions.split(df.col(str[1]), str[3]));
	}

	/**
	 * Drop the specified column
	 * 
	 * @param df
	 * @param commande
	 * @param log
	 * @return
	 */
	public Dataset<Row> drop(Dataset<Row> df, String commande, Logger log) {

		String[] str = commande.split(" ");

		return df.drop(str[3]);
	}

	/**
	 * Execute a SQL order by
	 * 
	 * @param df
	 * @param column
	 * @return
	 */
	public Dataset<Row> orderBy(Dataset<Row> df, String commande, Logger log) {

		String[] str = commande.split(" ");

		return df.orderBy(str[1]);
	}

	/**
	 * Ajoute une colonne contenant la date du passage dans le processeur
	 * 
	 * @param df
	 * @param column
	 * @param log
	 * @return
	 */
	public Dataset<Row> addTimeStamp(Dataset<Row> df, String column, Logger log) {

		String[] str = column.split(" ");

		return df.withColumn(str[1], functions.current_timestamp());

		/*
		 * SparkSession ss = SparkSession.builder().getOrCreate();
		 * df.createOrReplaceTempView("df");
		 * 
		 * // Création de la fonction UDF qui donne un nouveau timestamp pour //
		 * chaque row ss.udf().register("timestamp", new UDF1<Row, String>() {
		 * 
		 * @Override public String call(Row r) { return
		 * Instant.now().toString(); }
		 * 
		 * }, DataTypes.StringType);
		 * 
		 * // On ajoute la colonne avec les nouvelles infos (on fait l'UDF sur
		 * la // première colonne, vu que celle que l'on veut créer n'existe pas
		 * // encore)
		 * 
		 * Dataset<Row> df2 = ss.sql("SELECT *, timestamp(" +
		 * df.columns()[0].toString() + ") AS " + str[1] + " FROM df");
		 * df2.show(); return df2;
		 */
	}

	public Dataset<Row> SQL(Dataset<Row> df, String commande, Logger log) {

		String[] str = commande.split(" ");
		SparkSession ss = SparkSession.builder().getOrCreate();
		df.createOrReplaceTempView(str[1]);

		return ss.sql(str[2]);

	}

	/**
	 * Ajoute une colonne contenant un UID à chaque row
	 * 
	 * @param df
	 * @param column
	 * @param log
	 * @return
	 */
	public Dataset<Row> addUID(Dataset<Row> df, String column, Logger log) {

		String[] str = column.split(" ");
		log.warn(str.length + " " + str[2]);
		df.show();

		return df.withColumn(str[1], functions.monotonically_increasing_id());

	}

	public Dataset<Row> joinIn(Dataset<Row> df, String commande, Logger log) {

		// on récupère le joinIn et l'index, le nbColonne, la colonne 1 (et 2)
		String[] str = commande.split(" ");

		SparkSession ss = SparkSession.builder().getOrCreate();

		// Si on a qu'une colonne
		if (str[2].equals("1")) {
			// on récupupère la requete
			String request = commande.substring(str[0].length() + str[1].length() + str[2].length() + str[3].length());
			// récupération des données
			Dataset<Row> dfFromEs = ReadInput.readDataFromElastic(ss, str[1], request);
			// Join sur une seule colonne
			return df.join(dfFromEs, str[3]);

		} else {
			// on récupère la requete
			String request = commande
					.substring(str[0].length() + str[1].length() + str[2].length() + str[3].length() + str[4].length());
			// récupération des données
			Dataset<Row> dfFromEs = ReadInput.readDataFromElastic(ss, str[1], request);
			// execution de la jointure
			return df.join(dfFromEs, df.col(str[3]).equalTo(dfFromEs.col(str[4])));
		}
	}

	/**
	 * 
	 * Do collaborative filtering on an opensourcevendor dataset
	 * 
	 * @param df
	 * @param commande
	 * @param log
	 * @return
	 */
	public Dataset<Row> collaborativeFiltering(Dataset<Row> df, String commande, Logger log) {

		// Création de la sesison spark
		SparkSession ss = SparkSession.builder().getOrCreate();

		// Création de la vue
		df.createOrReplaceTempView("df");

		// On récupère juste les données qui nous intéresse
		Dataset<Row> newDf = ss.sql(
				"select session.session_id, url, count(*) as rating from df where session.session_id is not null group by session.session_id, url order by session_id, rating desc");

		// Création des indexers
		StringIndexerModel urlIndexer = new StringIndexer().setInputCol("url").setOutputCol("url_indexed").fit(newDf);
		StringIndexerModel sessionIndexer = new StringIndexer().setInputCol("session_id")
				.setOutputCol("session_id_indexed").fit(newDf);
		StringIndexerModel ratingIndexer = new StringIndexer().setInputCol("rating").setOutputCol("rating_indexed")
				.fit(newDf);

		// Random seed pas random pour reproduire
		long randomSeed = 25L;

		// Création de l'algo
		ALS als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("session_id_indexed").setItemCol("url_indexed")
				.setRatingCol("rating").setSeed(randomSeed);

		// Création des trucs pour récupérer les résultats
		IndexToString urlConverter = new IndexToString().setInputCol("url_indexed").setOutputCol("url_unindexed")
				.setLabels(urlIndexer.labels());
		IndexToString sessionConverter = new IndexToString().setInputCol("session_id_indexed")
				.setOutputCol("session_id_unindexed").setLabels(sessionIndexer.labels());
		IndexToString ratingConverter = new IndexToString().setInputCol("rating_indexed")
				.setOutputCol("rating_unindexed").setLabels(ratingIndexer.labels());

		// Création du pipeline d'execution de l'algo
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { urlIndexer, sessionIndexer, ratingIndexer,
				als, urlConverter, sessionConverter, ratingConverter });

		// split en training/test
		double[] weight = { 0.8, 0.2 };
		Dataset<Row>[] ldf = newDf.randomSplit(weight);
		// Création du modèle avec le set de donénes du training
		Model model = pipeline.fit(ldf[0]);

		// vérif avec le test
		Dataset<Row> predictions = model.transform(ldf[1]);
		predictions.show(10, false);

		// Nettoyage des prédictions
		Dataset<Row> cleaned_predictions = predictions.na().drop();

		// Création de l'evaluateur
		RegressionEvaluator evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating")
				.setPredictionCol("prediction");

		// Evaluation
		double rmse = evaluator.evaluate(cleaned_predictions);

		log.warn("RMSE : " + rmse);

		// Je sais pas trop à quoi ça sert
		cleaned_predictions.selectExpr("session_id", "url", "rating", "prediction",
				"rating - prediction residual_error", " (rating - prediction) / " + rmse + " within_rmse")
				.createOrReplaceTempView("rmseEvaluation");

		// Affichage des RMSE, normalement ça indique des trucs
		ss.sql("select * from rmseEvaluation").show(3);
		log.warn("within 1 rmse (should be > 68%) :"
				+ (100 * ss.sql("select * from rmseEvaluation where abs(within_rmse) < 1").count()
						/ (double) cleaned_predictions.count()));
		log.warn("within 2 rmse (should be > 95%) :"
				+ (100 * ss.sql("select * from rmseEvaluation where abs(within_rmse) < 2").count()
						/ (double) cleaned_predictions.count()));

		return predictions;
	}

}
