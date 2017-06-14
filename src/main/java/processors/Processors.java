package processors;

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

public class Processors {

	public Processors() {

	}

	public Dataset<Row> append(Dataset<Row> df, String commande, Logger log) {

		String[] str = commande.split(" ");

		String nomColonne = str[1] + "-" + str[2];
		if (str.length >= 4)
			nomColonne = str[3];

		return df.withColumn(nomColonne, functions.concat(df.col(str[1]), df.col(str[2])));

	}

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

	public Dataset<Row> stringToDate(Dataset<Row> df, String commande, Logger log) {

		String[] str = commande.split(" ");

		return df.withColumn(str[2], functions.from_utc_timestamp(df.col(str[1]), "+01:00"));

	}

	public Dataset<Row> split(Dataset<Row> df, String commande, Logger log) {

		String[] str = commande.split(" ");

		return df.withColumn(str[2], functions.split(df.col(str[1]), str[3]));
	}

	public Dataset<Row> drop(Dataset<Row> df, String commande, Logger log) {

		String[] str = commande.split(" ");

		return df.drop(str[3]);
	}

	public Dataset<Row> aggregate(Dataset<Row> df, String commande, Logger log) {
		/*
		 * val sessionUrlsDf = spark.
		 * sql("select session.session_id, url, count(*) as rating from cleanedUrlFbPageviewDf where session.session_id is not null group by session.session_id, url order by session_id, rating desc"
		 * )
		 */
		return null;
	}

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
