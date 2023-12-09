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

    public static String prettyPrintJsonUsingDefaultPrettyPrinter(String uglyJsonString) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        Object jsonObject = objectMapper.readValue(uglyJsonString, Object.class);
        String prettyJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
        return prettyJson;
    }

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


        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Using WatermarkStrategy for event time
        DataStream<String> kafkaDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Processing logic goes here
        // TODO: Add your processing logic here to process json messages from Kafka
//        DataStream<String> processedDataStream = kafkaDataStream
//                .map(jsonString -> {
//                    // Parse the JSON string into a JsonNode
//                    ObjectMapper objectMapper = new ObjectMapper();
//                    JsonNode jsonNode = objectMapper.readTree(jsonString);
//
//                    // Perform your processing on the JsonNode
//                    // For example, extract a field and modify it
//                    String modifiedField = jsonNode.get("user").asText() + "_modified";
//
//                    // Create a new JsonNode with the modified field
//                    JsonNode modifiedJsonNode = jsonNode.with(modifiedField);
//
//                    // Serialize the modified JsonNode back to a JSON string
//                    return objectMapper.writeValueAsString(modifiedJsonNode);
//                });

        // printing the messages to the console
//        String formattedDataStream = prettyPrintJsonUsingDefaultPrettyPrinter(kafkaDataStream.toString());

//        kafkaDataStream.getType();
        kafkaDataStream.print();
//        System.out.println(formattedDataStream);

        // Execute the Flink job
        env.execute("Kafka DataStream Example");

    }
}
