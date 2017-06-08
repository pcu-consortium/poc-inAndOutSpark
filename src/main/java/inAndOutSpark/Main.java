package inAndOutSpark;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import config.Configuration;
import config.Sortie;
import processors.Processors;

/**
 * TO DO : Fonction filter Ajouter une boucle permettant d'avoir plusieurs
 * filtres / un where avec des AND Mettre le calcul de la requete dans
 * Configuration
 * 
 * @author Thomas Estrabaud - Smile
 *
 */

public class Main {

	static Broadcast<Configuration> Bc;
	static SparkConf sparkConf = null;
	static JavaSparkContext jsc = null;
	static SparkSession ss = null;
	static LogManager logManager = new LogManager();
	static Logger log = LogManager.getRootLogger();

	/**
	 * Méthode principale. Appelle les différentes étapes
	 * 
	 * @param args
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 */
	public static void main(String[] args)
			throws JsonParseException, JsonMappingException, IOException, NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		// PostDo de Do de prédo
		postDo(Do(preDo(args)));

	}

	/**
	 * Initialise les différents éléments (sparkConf, JavaSparkContext)
	 * 
	 * @param args
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public static void init(String[] args) {

		try {
			sparkConf = new SparkConf().setAppName("Word Count Demo").setMaster("local");

			jsc = new JavaSparkContext(sparkConf);

			ss = SparkSession.builder().getOrCreate();

			log.warn("Initialisation - Successful");
		} catch (Exception e) {
			log.warn("Initialisation - Unsuccessful");
			e.printStackTrace();
		}

	}

	/**
	 * Lit le fichier de conf et en créé un objet java
	 * 
	 * @param args
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public static void readConf(String[] args) {

		try {
			// Décommenter afin de demander le fichier de conf (pour le moment
			// on
			// file le fichier de conf en dur)
			/*
			 * if (args.length < 1) {
			 * System.err.println("Usage: JavaWordCount <file>");
			 * System.exit(1); }
			 */

			JavaPairRDD<String, String> conf = jsc.wholeTextFiles("conf.yml");

			// Création du mapper
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

			// On crée l'objet config à envoyer en broadcast
			Configuration c = mapper.readValue(conf.values().first().toString(), Configuration.class);
			// System.out.println(conf.first());
			// On envoit l'objet en broadcast
			Bc = jsc.broadcast(c);
			log.warn("Read config - Succesful");
		} catch (Exception e) {
			log.warn("Read config - Unsuccessful");
			e.printStackTrace();
		}

	}

	/**
	 * Lit les données contenues dans les files/fichiers décrits dans le fichier
	 * de conf donné en entrée
	 * 
	 * @return Les données lues dans une map <NomDuFichier, DataDuFichier>
	 */
	public static HashMap<String, Dataset<Row>> readData() {

		try {
			// On récupère le fichier de config
			Configuration conf = Bc.getValue();

			// Création des dataframes par source
			HashMap<String, Dataset<Row>> dfs = new HashMap<String, Dataset<Row>>();

			// Remplissage des dataframes
			for (int i = 0; i < conf.getIn().size(); i++) {
				dfs.put(conf.getIn().get(i).getNom(), ss.read().json(conf.getIn().get(i).getNom()));
			}

			// Affichage des dataframe et de leur schéma
			/*
			 * for (Iterator<Entry<String, Dataset<Row>>> iterator =
			 * dfs.entrySet().iterator(); iterator.hasNext();) { Map.Entry
			 * mapEntry2 = iterator.next(); ((Dataset<Row>)
			 * mapEntry2.getValue()).show(50, false); ((Dataset<Row>)
			 * mapEntry2.getValue()).printSchema(); }
			 */

			log.warn("Read data - Successful");
			return dfs;
		} catch (Exception e) {
			log.warn("Read data - Unsuccessful");
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Fonction appelant les sous-parties du pré-Do A ne pas modifier par
	 * l'utilisateur
	 * 
	 * @param args
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public static HashMap<String, Dataset<Row>> preDo(String[] args)
			throws JsonParseException, JsonMappingException, IOException {

		// Configuration du spark
		init(args);

		// On lit le fichier source et on le met dans UN String qu'on répartit
		// sur tous les noeuds
		readConf(args);

		// return readData();
		return filter(readData());
	}

	/**
	 * Applique une filtre (pour le moment une requete SQL) sur le/les
	 * dataframe(s)
	 * 
	 * @param entrees
	 * @return
	 */
	public static HashMap<String, Dataset<Row>> filter(HashMap<String, Dataset<Row>> entrees) {

		try {
			// On récupère la config
			Configuration conf = Bc.getValue();
			// Création de
			HashMap<String, Dataset<Row>> entreesFiltrees = new HashMap<String, Dataset<Row>>();
			String from = "";
			int i = 0;

			// Pour chacun des dataset on applique la requete SQL associée
			for (Entry<String, Dataset<Row>> entry : entrees.entrySet()) {
				// On récupère sur quelle table on fait le FROM
				from = entry.getKey();
				// S'il y a une requete SQL à faire
				if (conf.getIn().get(i).isThereASQLRequest()) {
					// on crée la vue sur laquelle on travaille
					entry.getValue().createOrReplaceTempView(from);
					// on effectue la requete
					Dataset<Row> sqlDF = ss.sql(conf.getIn().get(i).getRequeteSQL(from));
					// on ajoute le résultat de la requete à l'objet à renvoyer
					entreesFiltrees.put(entry.getKey().toString(), sqlDF);
				} else // s'il n'y a pas de requete, on laisse tout ce qu'il y
						// avait
						// de base
					entreesFiltrees.put(entry.getKey().toString(), (Dataset<Row>) entry.getValue());
				i++;
			}
			// Affichage des résultats
			/*
			 * for (Entry<String, Dataset<Row>> entry :
			 * entreesFiltrees.entrySet()) { entry.getValue().show(50, false); }
			 */
			log.warn("Application Filter - Successful");
			return entreesFiltrees;
		} catch (Exception e) {
			log.warn("Application Filter - Unsuccessful");
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Fonction les différentes étapes de traitement Modifiable par
	 * l'utilisateur
	 * 
	 * @param entrees
	 * @return
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	public static HashMap<String, Dataset<Row>> Do(HashMap<String, Dataset<Row>> entrees) throws NoSuchMethodException,
			SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		try {
			Configuration conf = Bc.getValue();

			Method method = null;
			Processors p = new Processors();

			// pour chaque source
			for (int i = 0; i < conf.getOperations().size(); i++) {
				entrees.get(conf.getOperations().get(i).getNom_source()).show(50, false);
				// pour chaque opération par source
				for (int j = 0; j < conf.getOperations().get(i).getOperations().size(); j++) {
					// on sauvegarde le nom de la source sur laquelle on
					// travaille
					// (la clé de la map)
					String backUp = conf.getOperations().get(i).getNom_source();
					// Si on ne fait pas des opérations sur plusieurs sources

					// on récupère le nom du processor à effectuer (premier
					// élement du string)
					method = p.getClass().getMethod(conf.getOperations().get(i).getOperations().get(j).split(" ")[0],
							Dataset.class, String.class);
					// on récupère le nouveau dataset
					Dataset<Row> newDF = (Dataset<Row>) method.invoke(p, entrees.get(backUp),
							conf.getOperations().get(i).getOperations().get(j));
					// on enleve l'ancien
					entrees.remove(backUp);
					// on ajoute le nouveau
					entrees.put(backUp, newDF);
				}
				// pour chaque opération multi sources par sources
				for (int j = 0; j < conf.getOperations().get(i).getOperations_multi_sources().size(); j++) {
					String backUp = conf.getOperations().get(i).getNom_source();
					// Si on ne fait pas des opérations sur plusieurs sources

					// on récupère le nom du processor à effectuer (premier
					// élement du string)
					method = p.getClass().getMethod(
							conf.getOperations().get(i).getOperations_multi_sources().get(j).split(" ")[0],
							HashMap.class, String.class);
					// on récupère le nouveau dataset
					Dataset<Row> newDF = (Dataset<Row>) method.invoke(p, entrees,
							conf.getOperations().get(i).getOperations_multi_sources().get(j));
					// on enleve l'ancien
					entrees.remove(backUp);
					// on ajoute le nouveau
					entrees.put(backUp, newDF);
				}

				entrees.get(conf.getOperations().get(i).getNom_source()).show(50, false);
			}

			// reflexion pour appeler les opérations
			/*
			 * try { conf.getClass().getMethod(conf.getOperations().get(0).
			 * getOperations() .get(0)); method =
			 * t.getClass().getMethod(conf.getOperations().get(0).getOperations(
			 * ). get(0)); } catch (Exception e) { e.printStackTrace(); } try {
			 * 
			 * // System.out.println(method.invoke(t)); } catch
			 * (IllegalAccessException | IllegalArgumentException |
			 * InvocationTargetException e) { e.printStackTrace(); }
			 */

			// for(int j = 0; j < conf.getoutFiles().size(); j++)
			// entrees.get(j).saveAsTextFile(conf.getoutFiles().get(j));

			/*
			 * JavaRDD<String> textFile = jsc.textFile("loremipsum.txt");
			 * 
			 * JavaPairRDD<String, Integer> counts = textFile .flatMap(s ->
			 * Arrays.asList(s.split(" ")).iterator()) .mapToPair(word -> new
			 * Tuple2<>(word, 1)) .reduceByKey((a, b) -> a + b);
			 * counts.saveAsTextFile("test");.
			 */
			log.warn("Do - Successful");
			return entrees;
		} catch (Exception e) {
			log.warn("Do - Unsuccessful");
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Méthode écrivant les résultats ( dans des fichiers/files kafka)
	 * 
	 * @return
	 */
	public static int postDo(HashMap<String, Dataset<Row>> entrees) {

		try {
			Configuration conf = Bc.getValue();

			for (Sortie sor : conf.getOut()) {
				for (String str : sor.getFrom()) {
					entrees.get(str).write().json(sor.getNom() + "/" + str);
				}
			}

			/*
			 * for (Entry<String, Dataset<Row>> entry : entrees.entrySet()) {
			 * entry.getValue().write().json("output-" + entry.getKey());
			 * 
			 * }
			 */
			log.warn("postDo - Successful");
		} catch (Exception e) {
			log.warn("postDo - Unsuccessful");
			e.printStackTrace();
		}
		return 0;
	}
}