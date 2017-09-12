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
import config.TypeConnexion;
import inputOutput.ReadInput;
import inputOutput.WriteOutput;
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
	static String inputFolder = "";
	static String outputFolder = "";
	static String confFile = "";
	static String importFolder = "";

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
			// si on execute depuis eclipse
			if (args.length >= 4)
				sparkConf = new SparkConf().setAppName("inAndOutSpark").setMaster(args[3]);
			// si on execute en spark-submit
			else
				sparkConf = new SparkConf().setAppName("inAndOutSpark");

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
			JavaPairRDD<String, String> conf;
			JavaPairRDD<String, String> importInput;

			if (args.length < 3) {
				log.warn("Not enough parameters");
				log.warn("USAGE : [Configuration file] [Input Folder] [Output Folder]");
			} else {
				confFile = args[0];
				inputFolder = args[1];
				outputFolder = args[2];
			}

			conf = jsc.wholeTextFiles(confFile);

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
				if (conf.getIn().get(i).getType().equals(TypeConnexion.FOLDER))
					dfs.put(conf.getIn().get(i).getNom(),
							ReadInput.readJSONFromFile(ss, inputFolder + conf.getIn().get(i).getNom() + "/*"));
				else if (conf.getIn().get(i).getType().equals(TypeConnexion.FILE))
					dfs.put(conf.getIn().get(i).getNom(),
							ReadInput.readJSONFromFile(ss, inputFolder + conf.getIn().get(i).getNom()));
				else if (conf.getIn().get(i).getType().equals(TypeConnexion.PARQUET))
					dfs.put(conf.getIn().get(i).getNom(),
							ReadInput.readDataFromParquetFile(ss, inputFolder + conf.getIn().get(i).getNom()));
				else if (conf.getIn().get(i).getType().equals(TypeConnexion.ELASTICSEARCH))
					dfs.put(conf.getIn().get(i).getNom(), ReadInput.readDataFromElastic(ss,
							conf.getIn().get(i).getIndex(), conf.getIn().get(i).getRequest()));
				else
					dfs.put(conf.getIn().get(i).getNom(), ReadInput.readJSONFromKafka(ss,
							conf.getIn().get(i).getTopic(), conf.getIn().get(i).getIpBrokers()));
				dfs.get(conf.getIn().get(i).getNom()).show(false);

			}

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
				// pour chaque processeur
				for (int j = 0; j < conf.getOperations().get(i).getProcessors().size(); j++) {

					Dataset<Row> newDF;

					// Si c'est un processeur multi_sources
					if (conf.getOperations().get(i).getProcessors().get(j).startsWith("multi_sources")) {
						// on récupère le nouveau dataset
						newDF = executeProcessorsMultiSources(entrees, i, j);
					}
					// Si on est sur une opération mono source
					else {
						// on récupère le nouveau dataset
						newDF = executeProcessorsMonoSource(entrees, i, j);
					}

					// Si les input/output sont indentiques on remplace les
					// données
					if (conf.getOperations().get(i).isOutputSameAsInput()) {
						entrees.remove(conf.getOperations().get(i).getInput_source());
						entrees.put(conf.getOperations().get(i).getInput_source(), newDF);
					}
					// Sinon on crée un nouvel output
					else {
						entrees.put(conf.getOperations().get(i).getOutput_source(), newDF);
					}
					entrees.get(conf.getOperations().get(i).getInput_source()).show(50, false);
				}
			}
			log.warn("Do - Successful");
			return entrees;
		} catch (Exception e) {
			log.warn("Do - Unsuccessful");
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Function that detect the processor needed, execute it and send the result
	 * back. - It uses multiple dataframes as sources
	 * 
	 * @param Hdf
	 * @param i
	 * @param j
	 * @return
	 */
	public static Dataset<Row> executeProcessorsMultiSources(HashMap<String, Dataset<Row>> Hdf, int i, int j) {

		try {
			Configuration conf = Bc.getValue();
			Processors p = new Processors();
			Method method = p.getClass().getMethod(conf.getOperations().get(i).getProcessors().get(j).split(" ")[1],
					HashMap.class, String.class, Logger.class);
			// on récupère le nouveau dataset
			return (Dataset<Row>) method.invoke(p, Hdf, conf.getOperations().get(i).getProcessors().get(j), log);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * Function that detect the processor needed, execute it and send the result
	 * back. - It uses only one dataframe as source
	 * 
	 * @param Hdf
	 * @param i
	 * @param j
	 * @return
	 */
	public static Dataset<Row> executeProcessorsMonoSource(HashMap<String, Dataset<Row>> Hdf, int i, int j) {

		try {
			Configuration conf = Bc.getValue();
			Processors p = new Processors();
			Method method = p.getClass().getMethod(conf.getOperations().get(i).getProcessors().get(j).split(" ")[0],
					Dataset.class, String.class, Logger.class);
			// on récupère le nouveau dataset
			return (Dataset<Row>) method.invoke(p, Hdf.get(conf.getOperations().get(i).getInput_source()),
					conf.getOperations().get(i).getProcessors().get(j), log);
		} catch (Exception e) {
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
			int i = 0;
			Configuration conf = Bc.getValue();
			for (Sortie sor : conf.getOut()) {
				for (String str : sor.getFrom()) {
					log.warn(str);
					entrees.get(str).show();
					if (conf.getOut().get(i).getType().equals(TypeConnexion.FILE))
						WriteOutput.printToFs(entrees.get(str), outputFolder + sor.getNom() + "/" + str);
					else if (conf.getOut().get(i).getType().equals(TypeConnexion.KAFKA))
						WriteOutput.printToKafka(entrees.get(str), conf.getOut().get(i).getTopic(), "");
					else if (conf.getOut().get(i).getType().equals(TypeConnexion.PARQUET))
						WriteOutput.printParquetFile(entrees.get(str), outputFolder + sor.getNom() + "/" + str);
					else
						WriteOutput.printToES(entrees.get(str), conf.getOut().get(i).getIndex());
				}
				i++;
			}
			log.warn("postDo - Successful");
		} catch (Exception e) {
			log.warn("postDo - Unsuccessful");
			e.printStackTrace();
		}

		return 0;
	}
}