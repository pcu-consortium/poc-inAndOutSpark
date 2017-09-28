package inputOutput;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.struct;

import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import com.databricks.spark.avro.SchemaConverters;

import inAndOutSpark.Main;

/**
 * Class used to read data from somewhere
 * 
 * @author Thomas Estrabaud - Smile
 *
 */
public class ReadInput {

   private static Logger log = LogManager.getLogger(Main.class); // on log4 like Spark ; TODO rather slf4j on logback like pcu ?


	/**
	 * Read json data from a file/folder
	 * 
	 * @param ss
	 * @param path
	 *            The path of the file/folder
	 * @return
	 */
	public static Dataset<Row> readJSONFromFile(SparkSession ss, String path) {

		return ss.read().json(path);
	}

	/**
	 * Read data from a kafka topic
	 * 
	 * @param ss
	 * @param topic
	 *            Topic to read from
	 * @param IpBrokers
	 *            Ip of the brokers we read from
	 * @return
	 */
	public static Dataset<Row> readJSONFromKafka(SparkSession ss, String topic, String schema, boolean isStreaming, String startingOffsets) {
	   String format = "json"; // even using avro schema ; avro serialization would only require to add de/serializers

		Dataset<Row> ds;
		if (!isStreaming) {
		   DataFrameReader dfr = ss.read().format("kafka")
               .options(Main.ss.conf().getAll()) //.option("kafka.bootstrap.servers", IpBrokers) // rather at init (single Kafka)
               .option("subscribe", topic);
		   ds = dfr.load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic",
						"partition", "offset", "timestamp", "timestampType");

         ///if (schema == null) {
            // no explicit schema, so let's parse the json value and guess its schema using introspection :
   	      Dataset<String> stringDs = ds.select(ds.col("value")).map((Row row) -> row.mkString(), Encoders.STRING()); // TODO from_json udf else error toJavaRDD() : org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start();;
   	      ds = ss.read().json(stringDs); // doesn't work in streaming
   	      // therefore explicit format
         ///}
	      ds.show(false); // NB. does not work in streaming
/*
|null|{"test":"1"} |test |0        |0     |2017-09-25 11:17:12.649|0            |
|null|{"test":"2"} |test |0        |1     |2017-09-25 11:23:49.943|0            |
 */
		} else {
         DataStreamReader dsr = ss.readStream().format("kafka")
               .options(Main.ss.conf().getAll()) //.option("kafka.bootstrap.servers", IpBrokers) // rather at init (single Kafka)
               .option("subscribe", topic); // or .option("subscribePattern", "topic.*")
         if (startingOffsets != null) {
            dsr.option("startingOffsets", startingOffsets);
            //.option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
            //.option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
         }
         ds = dsr.load();
         
         ds.printSchema();
         /*
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)
          */

         if (schema == null) {
            // schema is required in streaming, therefore defaulting to topic :
            schema = topic; // "test";
            log.warn("");
         }
		}

		if (schema != null) {
		   // explicit schema to parse the value with :
		   
         // parsing avro value :
         // value.deserializer can't be set, values are always deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame operations to explicitly deserialize the values
         // see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
         //dfr = dfr.schema(schema); // does not deserialize
   		
   		// parsing JSON value :
   		// DON'T use RDD ss.read().json(ds.select(ds.col("value")).toJavaRDD().map(v1 -> v1.mkString()))
   		// rather from_json udf else error toJavaRDD() : org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start();;
   		// https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html
   		// alas this means we can't auto detect schema, but in streaming mode this would anyway be wrong for any upcoming data, i.e. streaming mode requires explicit schema in and of itself
   		
         Schema avroSchema = Main.typeSchemaMap.get(schema);
         StructType valueSchema = (StructType) SchemaConverters.toSqlType(avroSchema).dataType(); // TODO cache ?
         /*
         StructType valueSchema = new StructType(new StructField[] {
               new StructField("test", DataTypes.StringType, true, null)
         });
         */
         
         if ("json".equals(format)) {
            // parsing json AND moves original columns (including unparsed value) below "kafka." :
            HashMap<String, String> jsonOptions = new HashMap<String,String>();
            //jsonOptions.put("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"); // default yyyy-MM-dd'T'HH:mm:ss.SSSZZ https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/DataFrameReader.html
            ds = ds.select(from_json(col("value").cast("string"), valueSchema, jsonOptions).alias("parsed_value"),
                  struct("*").alias("kafka"));
            ds = ds.select("parsed_value.*", "kafka");
         } else { // if ("avro".equals(format))
            // TODO use or similar to https://github.com/Tubular/confluent-spark-avro
            //ds = ds.select(col("key"), from_avro(col("value").cast("string"), valueSchema, jsonOptions).alias("parsed_value")); // also drops original "value" column
         } // else string ?
		}
      ds.printSchema();
      /*
root
 |-- parsed_value: struct (nullable = true)
 |    |-- test: string (nullable = true)
       */
		log.warn(topic); // + " " + IpBrokers
		return ds;
	}

	public static Dataset<Row> readDataFromElastic(SparkSession ss, String index, String request) {
	   // NB. nodes configured at init (single ES)
		return JavaEsSparkSQL.esDF(ss, index, request);
	}

	public static Dataset<Row> readDataFromParquetFile(SparkSession ss, String path) {

		return ss.read().parquet(path);

	}
}
