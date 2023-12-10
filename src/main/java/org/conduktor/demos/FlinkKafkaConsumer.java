package org.conduktor.demos;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkKafkaConsumer {

    public static void main(String[] args) throws Exception {

        String brokers = "localhost:9092";
        String kafkaTopic = "wikimedia-changes";

        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        JsonDeserializationSchema<SomePojo> jsonFormat=new JsonDeserializationSchema<>(SomePojo.class);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(kafkaTopic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Using WatermarkStrategy for event time
        DataStream<String> kafkaDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Processing logic goes here
        // TODO: Add your processing logic here to process json messages from Kafka
        DataStream<WikimediaChangeSchema> deserialzedDataStream = kafkaDataStream.map(jsonString -> {

            try
            {
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.readTree(jsonString);


                // Extracting the values from the json node
                JsonNode idNode = jsonNode.get("id");
                long id = (idNode == null) ? 0 : idNode.asLong();

                JsonNode typeNode = jsonNode.get("type");
                String type = (typeNode == null) ? "" : typeNode.asText();

                JsonNode userNode = jsonNode.get("user");
                String user = (userNode == null) ? "" : userNode.asText();

                // Creating a wikimedia change object
                return new WikimediaChangeSchema(id, type, user);

        }
            catch (JsonProcessingException e)
            {
                e.printStackTrace();
                return null;
            }
        });

        deserialzedDataStream.print();

        // Execute the Flink job
        env.execute("Kafka DataStream Example");

    }
}
