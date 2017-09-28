package inAndOutSpark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import config.Configuration;
import config.Entree;
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
   public static final String DURATION_PROP = "duration";

   private static Logger log = LogManager.getLogger(Main.class); // on log4 like Spark ; TODO rather slf4j on logback like pcu ?

	// Créer un objet properties
	static Broadcast<Configuration> Bc;
	static SparkConf sparkConf = null;
	static JavaSparkContext jsc = null;
	public static SparkSession ss = null;
	static JavaStreamingContext ssc = null;
	static String inputFolder = "";
	static String outputFolder = "";
	static String confFile = "";
	static String importFolder = "";
	/** ms, default 500 (anyway, nothing happens unless data comes in) */
	static long streamingDuration = 500;

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
	 * @throws InterruptedException 
	 */
	public static void main(String[] args)
			throws JsonParseException, JsonMappingException, IOException, NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException {

		// PostDo de Do de prédo
		postDo(Do(preDo(args)));

      if (ssc != null) { // there is at least one streaming input
         ssc.awaitTermination();
      }
	}

	/**
	 * Initialise les différents éléments (sparkConf, JavaSparkContext)
	 * 
	 * @param args TODO eclipse
	 * @param conf 
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public static void init(String[] args, Configuration conf) {

		try {
         sparkConf = new SparkConf();

         // defaults :
         sparkConf.set("spark.app.name", "inAndOutSpark"); // OPT could use end of file name
         //sparkConf.set("spark.master", "local"); // NOT in spark-submit mode, so provide it as (eclipse) args
         sparkConf.set("kafka.bootstrap.servers", "localhost:9092"); // "host1:port1,host2:port2"
         sparkConf.set("es.nodes", "localhost:9200");
         
         // override from conf :
         if (conf.getConf() != null) {
            for (Entry<Object, Object> entry : conf.getConf().entrySet()) {
               sparkConf.set((String) entry.getKey(), (String) entry.getValue());
            }
		   }

         // override from args :
         ArrayList<String> nonPropArgList = new ArrayList<String>();
		   for (int i = 3 ; i < args.length ; i++) {
		      String[] sparkConfKeyValue = args[i].split("=", 1);
		      if (sparkConfKeyValue.length == 2) {
		         sparkConf.set(sparkConfKeyValue[0], sparkConfKeyValue[1]);
		      } else {
		         nonPropArgList.add(args[i]);
		      }
		   }
         if (!nonPropArgList.isEmpty()) {
            sparkConf.set("spark.master", nonPropArgList.get(0));
         }
         log.warn("Initialisation - using spark conf :\n" + sparkConf.toDebugString()
               + "\nsome having been taken from other args :\n" + nonPropArgList);
		   
			// si on execute depuis eclipse
			/*if (args.length >= 4)
				sparkConf = new SparkConf().setAppName("inAndOutSpark").setMaster(args[3]);
			// si on execute en spark-submit
			else
				sparkConf = new SparkConf().setAppName("inAndOutSpark");*/

         ///sparkConf.set("spark.driver.allowMultipleContexts", "true"); // NOO if streaming rather new JavaStreamingContext(sparkContext)
			jsc = new JavaSparkContext(sparkConf);

			ss = SparkSession.builder().getOrCreate();
			
			// Main props init :
			if (sparkConf.contains(DURATION_PROP)) streamingDuration = Integer.parseInt(sparkConf.get(DURATION_PROP));

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
	 * @return 
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public static Configuration readConf(String[] args) {

		try {
			if (args.length < 3) {
				log.warn("Not enough parameters");
				log.warn("USAGE : [Configuration file] [Input Folder] [Output Folder]");
			} else {
				confFile = args[0];
				inputFolder = args[1];
				outputFolder = args[2];
			}

         String confString;
			/*
			JavaPairRDD<String, String> confRDD = jsc.wholeTextFiles(confFile);
			confString = confRDD.values().first().toString();
			*/
			try (FileInputStream confFis = new FileInputStream(confFile)) {
			   confString = IOUtils.toString(confFis);
			}

			// Création du mapper
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

			// On crée l'objet config à envoyer en broadcast
			Configuration c = mapper.readValue(confString, Configuration.class);
			
			// schemas :
			Files.list(Paths.get("avro")).map(p -> p.toFile()).filter(f -> f.getName().endsWith(".avsc")).filter(f -> f.isFile()).forEach(f -> {
   	      try (InputStream avroSchemaResourceIs = new FileInputStream(f)) {
   	         Schema schema = new Schema.Parser().parse(avroSchemaResourceIs);
   	         fileSchemaMap.put(schema.getFullName(), schema);
   	         for (Schema typeSchema : schema.getTypes()) {
   	            typeSchemaMap.put(typeSchema.getName(), typeSchema); // TODO fullName
   	         }
   	      } catch (IOException ioex) {
   	         log.error("Failed to load avro schema " + f.getAbsolutePath(), ioex);
   	      }
			});
         log.warn("Loaded avro schemas " + typeSchemaMap.keySet());

			// System.out.println(conf.first());
			log.warn("Read config - Succesful");
			return c;
		} catch (Exception e) {
			log.warn("Read config - Unsuccessful");
			throw new RuntimeException("Read config - Unsuccessful", e);
		}

	}
   private static HashMap<String,Schema> fileSchemaMap = new HashMap<String,Schema>();
   public static HashMap<String,Schema> typeSchemaMap = new HashMap<String,Schema>();

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
            Entree in = conf.getIn().get(i);
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
				else if (conf.getIn().get(i).getType().equals(TypeConnexion.KAFKA))
					dfs.put(in.getNom(), ReadInput.readJSONFromKafka(ss,
							in.getTopic(), in.getSchema(), false, null));
				else { // KAFKA_STREAM
				   if (ssc == null) {
				      ///ssc = new JavaStreamingContext("local[*]", "inAndOut", new Duration(5000)); // NOO SparkException: Only one SparkContext may be running in this JVM (see SPARK-2243). To ignore this error, set spark.driver.allowMultipleContexts = true. The currently running SparkContext was created at:
                  ssc = new JavaStreamingContext(jsc, new Duration(streamingDuration)); // TODO LATER several to allow different durations
                  // or JavaSparkContext.fromSparkContext(sc) https://stackoverflow.com/questions/34879414/multiple-sparkcontext-detected-in-the-same-jvm
				   }
               dfs.put(in.getNom(), ReadInput.readJSONFromKafka(ss,
                     in.getTopic(), in.getSchema(), true, in.getStartingOffsets()));
				}
				if (!conf.getIn().get(i).getType().equals(TypeConnexion.KAFKA_STREAM)) {
				   dfs.get(conf.getIn().get(i).getNom()).show(false);
				} // doesn't work in streaming

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
	 * @throws InterruptedException 
	 */
	public static HashMap<String, Dataset<Row>> preDo(String[] args)
			throws JsonParseException, JsonMappingException, IOException, InterruptedException {

      // On lit le fichier source et on le met dans UN String qu'on répartit
      // sur tous les noeuds
      Configuration conf = readConf(args);

      // Configuration du spark
      init(args, conf);
      
      // On envoit l'objet conf en broadcast
      Bc = jsc.broadcast(conf);

		HashMap<String, Dataset<Row>> readDataMap = readData();
		return filter(readDataMap);
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
					log.debug("postDo from : " + str);
					Dataset<Row> inDf = entrees.get(str);
					if (!inDf.isStreaming()) {
					   inDf.show();
					} // else show doesn't work in streaming
					if (conf.getOut().get(i).getType().equals(TypeConnexion.FILE))
						WriteOutput.printToFs(inDf, outputFolder + sor.getNom() + "/" + str);
					else if (conf.getOut().get(i).getType().equals(TypeConnexion.KAFKA) || conf.getOut().get(i).getType().equals(TypeConnexion.KAFKA_STREAM))
						WriteOutput.printToKafka(inDf, conf.getOut().get(i).getTopic());
					else if (conf.getOut().get(i).getType().equals(TypeConnexion.PARQUET))
						WriteOutput.printParquetFile(inDf, outputFolder + sor.getNom() + "/" + str);
					else
						WriteOutput.printToES(inDf, conf.getOut().get(i).getIndex(), conf.getOut().get(i).isKeepOriginal());
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