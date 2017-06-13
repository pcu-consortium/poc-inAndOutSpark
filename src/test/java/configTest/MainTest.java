package configTest;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import config.Configuration;
import config.Entree;
import config.Filtre;
import config.Format;
import config.Operation;
import config.Sortie;
import config.TypeConnexion;
import misc.Misc;
import misc.NiveauxLogs;

public class MainTest extends SharedJavaSparkContext {

	public Configuration setUpConfig() throws Exception {

		Filtre f1 = new Filtre("", "referer.domain = www.google.tn", "");
		Filtre f2 = new Filtre("", "", "SELECT * FROM b WHERE referer.domain = www.google.fr");
		List<Entree> in = new ArrayList<Entree>();
		in.add(new Entree("a", TypeConnexion.FILE, Format.JSON, f1));
		in.add(new Entree("b", TypeConnexion.FILE, Format.JSON, f2));
		List<Sortie> out = new ArrayList<Sortie>();
		out.add(new Sortie("c", TypeConnexion.FILE, Format.JSON, new ArrayList<String>()));
		out.add(new Sortie("d", TypeConnexion.FILE, Format.JSON, new ArrayList<String>()));
		List<Operation> ope = new ArrayList<Operation>();
		List<String> ope1 = new ArrayList<String>();
		ope1.add("ope1");
		ope1.add("test1.test2.test3");
		ope1.add("ope3");
		List<String> ope2 = new ArrayList<String>();
		ope2.add("ope4");
		ope2.add("ope5");
		// ope.add(new Operation("a", ope1, new ArrayList<String>()));
		// ope.add(new Operation("b", ope2, new ArrayList<String>()));

		return new Configuration(in, out, ope);

	}

	// @Test
	public void testLectureFichierConf() throws Exception {
		// A DECOMMENTER POUR FAIRE LE VRAI TEST

		Random rand = new Random();
		Configuration conf = setUpConfig();

		JavaPairRDD<String, String> config = jsc().wholeTextFiles("conf.yml");

		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

		// On crée l'objet config à envoyer en broadcast
		Configuration c = mapper.readValue(config.values().first().toString(), Configuration.class);

		List<String> temp = new ArrayList<String>();
		temp.add(conf.toString());
		JavaRDD<String> temp2 = jsc().parallelize(temp);
		temp2.saveAsTextFile("lala" + rand.nextInt(1000000000));
		System.out.println("Fait a la main = " + conf.toString());
		System.out.println("lu " + c.toString());
		System.out.println("a ecrire " + temp2.first().toString());
		System.out.println(conf.getIn().toString());
		// System.out.println(conf.getIn().get(1).getRequeteSQL("osef"));
		assertEquals(conf.toString(), c.toString());
	}

	// @Test
	public void testRequeteSQL() throws JsonParseException, JsonMappingException, IOException {

		// Creation de l'objet permettant de faire les requetes

		SparkSession ss = SparkSession.builder().getOrCreate();

		// Création de l'objet configuration
		JavaPairRDD<String, String> config = jsc().wholeTextFiles("conf.yml");
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		Configuration conf = mapper.readValue(config.values().first().toString(), Configuration.class);

		// Lecture des entrées
		HashMap<String, Dataset<Row>> entrees = readLaData(conf, ss);

		// Objet à renvoyer
		HashMap<String, Dataset<Row>> entreesFiltrees = new HashMap<String, Dataset<Row>>();
		// Nom de la colonne avec laquelle on fait le FROM
		String from = "a";
		int i = 0;
		/*
		 * entrees.get("a").createOrReplaceTempView(from); Dataset<Row> sqlDF =
		 * ss
		 * .sql("SELECT * FROM a WHERE visit.visitor_id = \"d2cf6994-74a0-c267-6d35-e1c9f4b2e8b8\""
		 * ); sqlDF.show(50, false);
		 */
		System.out.println("Taille " + entrees.size());
		// Pour chacun des dataset on applique la requete SQL associée

		for (Entry<String, Dataset<Row>> entry : entrees.entrySet()) {
			from = entry.getKey();
			System.out.println("ifirst = " + i);
			if (conf.getIn().get(i).isThereASQLRequest()) {
				System.out.println("i = " + i);
				entry.getValue().createOrReplaceTempView(from);
				System.out.println(conf.getIn().get(i).getRequeteSQL(from));
				Dataset<Row> sqlDF = ss.sql(conf.getIn().get(i).getRequeteSQL(from));
				entreesFiltrees.put(entry.getKey().toString(), sqlDF);
			} else
				entreesFiltrees.put(entry.getKey().toString(), (Dataset<Row>) entry.getValue());
			i++;
		}

		for (Entry<String, Dataset<Row>> entry : entreesFiltrees.entrySet()) {
			entry.getValue().show(50, false);
		}

	}

	// Copié collé de la fonction de lecture, juste c'est ici car c'est plus
	// simple
	public static HashMap<String, Dataset<Row>> readLaData(Configuration conf, SparkSession ss) {
		// Création des dataframes par source
		HashMap<String, Dataset<Row>> dfs = new HashMap<String, Dataset<Row>>();

		// Remplissage des dataframes
		for (int i = 0; i < conf.getIn().size(); i++)
			dfs.put(conf.getIn().get(i).getNom(), ss.read().json(conf.getIn().get(i).getNom()));

		return dfs;
	}

	// @Test
	public void testAppend() {

		SparkSession ss = SparkSession.builder().getOrCreate();

		Dataset<Row> df = ss.read().json("a");
		df.show();
		df.select(functions.concat(df.col("session.referrer.domain"), df.col("session.referrer.page"))).toDF().show(50,
				false);
	}

	// @Test
	public void testLectureOperations() throws JsonParseException, JsonMappingException, IOException {

		JavaPairRDD<String, String> config = jsc().wholeTextFiles("conf.yml");

		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

		// On crée l'objet config à envoyer en broadcast
		Configuration c = mapper.readValue(config.values().first().toString(), Configuration.class);

		System.out.println(c.toString());
	}

	@Test
	public void testLogs() {

		System.out.println(Misc.writeLog(NiveauxLogs.INFO, "Ceci est un test"));

	}

}
