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

//    public static String prettyPrintJsonUsingDefaultPrettyPrinter(String uglyJsonString) throws JsonProcessingException {
//        ObjectMapper objectMapper = new ObjectMapper();
//        Object jsonObject = objectMapper.readValue(uglyJsonString, Object.class);
//        String prettyJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
//        return prettyJson;
//    }

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
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.readTree(jsonString);

                // For example, extract a field and modify it
                long id = jsonNode.get("id").asLong();
                String type = jsonNode.get("type").asText();
                String user = jsonNode.get("user").asText();

                // Creating a wikimedia change object
                WikimediaChangeSchema wikimediaChangeSchema = new WikimediaChangeSchema(id, type, user);

                // print the object
//            System.out.println(wikimediaChangeSchema.toString());

                return wikimediaChangeSchema;
            }
            catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }

        });

//        kafkaDataStream.getType();
        deserialzedDataStream.print();
//        System.out.println(formattedDataStream);

        // Execute the Flink job
        env.execute("Kafka DataStream Example");

    }
}
