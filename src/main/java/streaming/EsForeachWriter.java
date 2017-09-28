package streaming;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.rest.RestService.PartitionWriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import inAndOutSpark.Main;
import inputOutput.WriteOutput;

public class EsForeachWriter extends ForeachWriter<Row> {
   private static final long serialVersionUID = -8534656621391425895L;

   private static Logger log = LogManager.getLogger(Main.class); // on log4 like Spark ; TODO rather slf4j on logback like pcu ?
   private static Log esLog = LogFactory.getLog(WriteOutput.class); // !!??
   
   private String esSettingsString;
   private StructType schema;
   private boolean keepOriginal = true; // emerged fields ?
   
   private transient PartitionWriter writer = null; // transient else error not serializable ; NB. ES Hadoop JacksonJsonGenerator uses org.codehaus.jackson.JsonGenerator
   private ObjectMapper mapper = new ObjectMapper(); // serializable
   
   public EsForeachWriter(String esSettingsString, StructType schema, boolean keepOriginal) {
      this.esSettingsString = esSettingsString;
      this.schema = schema;
   }
   
   @Override
   public void process(Row row) {
      if (writer == null) {
         Settings esSettings = new PropertiesSettings().load(this.esSettingsString);
         writer = RestService.createWriter(esSettings, 0, -1, esLog); // TODO taskContext.partitionId ?
      }
      //writer.repository.writeToIndex(row.getString(0)); // "{\"test\":\"es\"}"
      // NOT row else EsHadoopIllegalArgumentException: Spark SQL types are not handled through basic RDD saveToEs() calls; typically this is a mistake(as the SQL schema will be ignored). Use 'org.elasticsearch.spark.sql' package instead
      // at org.elasticsearch.spark.serialization.ScalaValueWriter.org$elasticsearch$spark$serialization$ScalaValueWriter$$doWrite(ScalaValueWriter.scala:111)
      try {
         String json_value = row.getString(0);
         String original_value = row.getString(1);
         //Row kafkaInfos = row.getString(2); // could still use kafka infos here
         
         @SuppressWarnings("unchecked")
         Map<String, Object> jsonObject = mapper.readValue(keepOriginal ? original_value : json_value, Map.class); // TODO better : row to map/list using schema
         if (keepOriginal) {
            // OPT enrich with emerged fields outside strict schema :
            ObjectReader jsonObjectUpdater = mapper.readerForUpdating(jsonObject); // https://stackoverflow.com/questions/9895041/merging-two-json-documents-using-jackson
            jsonObject = jsonObjectUpdater.readValue(json_value);
         }
         
         // sending to ES :
         // JSON object AND NOT String else NotXContentException: Compressor detection can only be called on some xcontent bytes
         // or compressed xcontent bytes https://github.com/elastic/elasticsearch-rails/issues/606
         writer.repository.writeToIndex(jsonObject);
      } catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
   @Override
   public boolean open(long partitionId, long version) {
      // TODO https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach
      return true; // do we need to write & process ? ex. not for already committed after partial failure
   }
   @Override
   public void close(Throwable errorOrNull) {
      if (errorOrNull != null) {
         log.error("Error closing ES streaming writer", errorOrNull);
      }
   }
}
