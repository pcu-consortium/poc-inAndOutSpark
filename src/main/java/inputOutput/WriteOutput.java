package inputOutput;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.mr.WritableBytesConverter;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.spark.cfg.SparkSettings;
import org.elasticsearch.spark.cfg.SparkSettingsManager;
import org.elasticsearch.spark.serialization.ScalaValueWriter;
import org.elasticsearch.spark.sql.EsSparkSQL;

import inAndOutSpark.Main;
import scala.collection.JavaConverters;
import scala.collection.mutable.Buffer;
import streaming.EsForeachWriter;

/**
 * Class used to write data
 * 
 * @author Thomas Estrabaud - Smile
 *
 */
public class WriteOutput {

   private static Logger log = LogManager.getLogger(Main.class); // on log4 like Spark ; TODO rather slf4j on logback like pcu ?
   private static Log esLog = LogFactory.getLog(WriteOutput.class); // !!??

	/**
	 * Write json data on the file system
	 * 
	 * @param entrees
	 * @param path
	 *            path to write to
	 */
	public static void printToFs(Dataset<Row> entrees, String path) {
      Dataset<Row> df = entrees;
      df.printSchema();
      if (df.schema().getFieldIndex("kafka").isDefined()) {
         // preparing df to output - taking out kafka info (should have been used in inAndOut operations) :
         // (filtering out kafka.* because .drop(col) doesn't work on nested column)
         df = df.select(columnsWithout("kafka", df)); // down
         df.printSchema();
      }
		df.write().json(path);
	}

	/**
	 * Write the data into Elasticsearch
	 * 
	 * @param entrees
	 *            data to write
	 * @param index
	 *            Elasticsearch's index to write to
	 * @param sparkConf 
	 */
	public static void printToES(Dataset<Row> entrees, String index, boolean keepOriginal) {
	   ;

      Dataset<Row> df = entrees;
      df.printSchema();

      if (df.schema().getFieldIndex("kafka").isDefined()) {
         /*
root
 |-- test: string (nullable = true)
 |-- kafka: struct (nullable = false)
 |    |-- key: binary (nullable = true)
 |    |-- value: binary (nullable = true)
 |    |-- topic: string (nullable = true)
 |    |-- partition: integer (nullable = true)
 |    |-- offset: long (nullable = true)
 |    |-- timestamp: timestamp (nullable = true)
 |    |-- timestampType: integer (nullable = true)
          */
         // preparing column to jsonize - taking out kafka info (should have been used in inAndOut operations) :
         // (filtering out kafka.* because .drop(col) doesn't work on nested column)
         df = df.select(struct(columnsWithout("kafka", df)).alias("parsed_value"), col("kafka")); // down
         
         // could still add kafka-specific info in ES :
         ///df = df.withColumn("parsed_value", struct(col("parsed_value.*"), struct(col("key"), col("timestamp")).alias("kafka"))); // & timestamp, offset ?
         /*
root
 |-- parsed_value: struct (nullable = false)
 |    |-- test: string (nullable = true)
 |-- kafka: struct (nullable = false)
 |    |-- key: binary (nullable = true)
 |    |-- value: binary (nullable = true)
 |    |-- topic: string (nullable = true)
 |    |-- partition: integer (nullable = true)
 |    |-- offset: long (nullable = true)
 |    |-- timestamp: timestamp (nullable = true)
 |    |-- timestampType: integer (nullable = true)
          */
         
      } // else input json was read in batch without schema
	   
      if (!df.isStreaming()) {
         if (df.schema().getFieldIndex("parsed_value").isDefined()) {
            df = df.select("parsed_value.*"); // up
            df.printSchema();
         }
         EsSparkSQL.saveToEs(df, index);
         
      } else {
         // preparing for ES :
         // to json : (or in ForeachWriter ?)
         // NOT df.toJSON() because calls toRDD() triggering org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start()
         df = df.select(to_json(col("parsed_value")).alias("json_value"), col("kafka.value").cast("string")); // , col("kafka")
         df.printSchema();
         /*
   root
   |-- json_value: string (nullable = true)
   |-- value: string (nullable = true)
          */
         
         // sending to ES :
         //EsSparkStreaming.saveToEs(df); // JavaEsSparkStreaming ; NOO works only on DStream and not dataframe
         
         SparkSettings sparkCfg = new SparkSettingsManager().load(Main.ss.sparkContext().conf());
         Settings esSettings = new PropertiesSettings().load(sparkCfg.save());
         //esSettings.setNodes(esNodes); // rather at init (single ES)
         
         // conf : TODO rather at init ?
         InitializationUtils.setValueWriterIfNotSet(esSettings, ScalaValueWriter.class, esLog); // or setOutputAsJson ; ScalaValueWriter
         // NOT NoOpValueWriter.class else EsHadoopIllegalStateException: Incorrect configuration - NoOpValueWriter should not have been called
         //esSettings.setProperty(ConfigurationOptions.ES_OUTPUT_JSON, "true"); // ... and not better
         InitializationUtils.setBytesConverterIfNeeded(esSettings, WritableBytesConverter.class, esLog);
         //InitializationUtils.setFieldExtractorIfNotSet(esSettings, , log); // DataFrameFieldExtractor.class
         esSettings.setProperty(ConfigurationOptions.ES_BATCH_SIZE_ENTRIES, "1"); // no batch in streaming mode ! else default 1000 see RestRepository.doWriteToIndex() l. 194
         
         esSettings.setResourceWrite(index); // must contain type ex. files/file, else EsHadoopIllegalArgumentException: invalid pattern given test/ below RestService.createWriter(RestService.java:566)
         EsForeachWriter esfw = new EsForeachWriter(esSettings.save(), df.schema(), keepOriginal);
         df.writeStream().foreach(esfw).start();
      }
	}

   public static void printToKafka(Dataset<Row> entrees, String topic) {

      Dataset<Row> df = entrees;
      df.printSchema();
      /*
root
 |-- parsed_value: struct (nullable = true)
 |    |-- test: string (nullable = true) 
       */

      if (df.schema().getFieldIndex("kafka").isDefined()) { // comes from kafka :
         // preparing column to jsonize - taking out kafka info (should have been used in inAndOut operations) :
         // (filtering out kafka.* because .drop(col) doesn't work on nested column)
         df = df.select(struct(columnsWithout("kafka", df)).alias("parsed_value"), col("kafka")); // down
         
         // could still add kafka-specific info in value :
         ///df = df.withColumn("parsed_value", struct(col("parsed_value.*"), struct(col("key"), col("timestamp")).alias("kafka"))); // & timestamp, offset ?

         // preparing df to output :
         //df = df.toJSON(); // NOO toJSON() calls toRDD() triggering org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start()
         df = df.withColumn("value", to_json(col("parsed_value")));
         df = df.select("kafka.key", "value"); // reusing key but NOT topic ! nor "kafka.partition", "kafka.offset", not even "kafka.timestamp", "kafka.timestampType" ?
         
      } else { // else input json was read in batch without schema
         //df = df.toJSON(); // NOO toJSON() calls toRDD() triggering org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start()
         df = df.select(to_json(struct("*")).alias("value"));
         // TODO "key" ? & "partition", "offset", "timestamp", "timestampType" ?
      }
      df = df.withColumn("topic", lit(topic));
		df.printSchema();
		/*
root
 |-- parsed_value: struct (nullable = true)
 |    |-- test: string (nullable = true)
 |-- value: string (nullable = true)
 |-- key: string (nullable = false)
		 */

		if (!df.isStreaming()) {
	      ///df.show(50, false);
   		df.write().format("kafka")
   		      .options(Main.ss.conf().getAll()) //.option("kafka.bootstrap.servers", ipBrokers) // rather at init (single Kafka)
   				.option("topic", topic).save(); // not if streaming else AnalysisException: 'write' can not be called on streaming Dataset/DataFrame
         // in which case the solution is : df.writeStream().foreach(new ForeachWriter<Row>() { process(row) ...
         // https://stackoverflow.com/questions/45113538/how-to-write-streaming-dataset-to-cassandra
         // http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach
   		
         // but if we want to do it right (writing in batches etc.) then we're better writing a new (ES etc.) streaming sink :
         // https://github.com/holdenk/spark-structured-streaming-ml/blob/master/src/main/scala/com/high-performance-spark-examples/structuredstreaming/CustomSink.scala
         
   		// and if we want to listen to ES writes (or their results), consuming a kafka topic is not
         // slower than calling a proxy ES API impl (unless listening is done in inAndOut postDoES() !)
   		
		} else {
		   // NB. show() does not work in streaming
         df.writeStream().format("kafka")
               .options(Main.ss.conf().getAll()) //.option("kafka.bootstrap.servers", IpBrokers) // rather at init (single Kafka)
               .option("topic", topic)
               .option("checkpointLocation", nextCheckpointLocation()).start();
		}

	}

   /**
    * 
    * @param entrees
    * @param path ex. people.parquet
    */
	public static void printParquetFile(Dataset<Row> entrees, String path) {
      Dataset<Row> df = entrees;
      df.printSchema();
      if (df.schema().getFieldIndex("kafka").isDefined()) {
         // preparing df to output - taking out kafka info (should have been used in inAndOut operations) :
         // (filtering out kafka.* because .drop(col) doesn't work on nested column)
         df = df.select(columnsWithout("kafka", df)); // down
         df.printSchema();
      }

		df.write().parquet(path);
	}

   private static int currentCheckpointLocationIndex = 0;
   private static String checkpointLocationPrefix = "kafkaCheckpoint";
   private static String nextCheckpointLocation() {
      return checkpointLocationPrefix + currentCheckpointLocationIndex++;
   }
   
   /**
    * TODO move to spark helper
    * column filtering helper, since df.drop(col) doesn't work on nested columns
    * @param df 
    * @return 
    */
   private static Buffer<Column> columnsWithout(String colName, Dataset<Row> df) {
      List<Column> columnsWithoutKafka = Arrays.asList(df.columns()).stream()
            .filter(fn -> !fn.equals(colName)).map(fn -> df.col(fn)).collect(Collectors.toList());
      return JavaConverters.asScalaBufferConverter(columnsWithoutKafka).asScala();
   }

}
