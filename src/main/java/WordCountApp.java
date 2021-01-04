import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 1. Stream from Kafka
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        // 2. Map Values lower case
        KTable<String,Long> wordCounts =  wordCountInput
                .mapValues(value -> value.toLowerCase())

        // 3. FlatMap values - split by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
        // 4. Select key to apply a key - name keys the same as values
                .selectKey((key, value) -> value)
        // 5. Group by key before aggregation
                .groupByKey()
        // 6. Count
                .count(Named.as("Counts"));

        // 7- send back to kafka
        wordCounts.toStream().to( "word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(),config);
        streams.start();

        //printing the topology

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
