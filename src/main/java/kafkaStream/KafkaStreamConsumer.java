package kafkaStream;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

public class KafkaStreamConsumer {

	public static void main(String[] args) {
		new KafkaStreamConsumer().start();
	}

	private void start() {
		Properties properties = new Properties();
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-consumer-1");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

		// creeer un kafka stream
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, String> kStream = streamsBuilder.stream("bdccTopic",
				Consumed.with(Serdes.String(), Serdes.String()));
		/*
		 * kStream.foreach((k,v)-> { System.out.println("key: " + k +" value: " + v); }
		 * );
		 */

		/*
		 * kStream .flatMapValues(textLine-> Arrays.asList(textLine.split("\\W+"))) //
		 * split .map((k,v)-> new KeyValue<>(k, v.toLowerCase())) // converti en
		 * miniscule .filter((k,v)-> v.equals("a") || v.equals("b") ) // recupere que
		 * "a" et "b" .foreach((k,v)-> { System.out.println("key: " + k +" value: " +
		 * v); });
		 */

		/*
		 * KStream<String, String> resultKStream = kStream .flatMapValues(textLine ->
		 * Arrays.asList(textLine.split("\\W+"))) // split .map((k, v) -> new
		 * KeyValue<>(k, v.toLowerCase())) // converti en miniscule .filter((k, v) ->
		 * v.equals("a") || v.equals("b")); // recupere que "a" et "b"
		 * 
		 * // renvoi resutlat vers un autre topic resultKStream.to("resultTopic",
		 * Produced.with(Serdes.String(), Serdes.String()));
		 * 
		 */
		/*
		 * KTable<String, Long> resultKStream = kStream .flatMapValues(textLine ->
		 * Arrays.asList(textLine.split("\\W+"))) // split .map((k, v) -> new
		 * KeyValue<>(k, v.toLowerCase())) // converti en miniscule .filter((k, v) ->
		 * v.equals("a") || v.equals("b")) // recupere que "a" et "b" .groupBy((k, v) ->
		 * v) // grouper par "a" et "b" .count(Materialized.as("count-analytics")); //
		 * creation d'une vue "count-analytics"
		 * 
		 * // renvoi resutlat d'un KTable vers un autre topic.Il faut d'abord convertir
		 * Ktable en KStream resultKStream.toStream().to("resultTopic",
		 * Produced.with(Serdes.String(), Serdes.Long()));
		 */

		KTable<Windowed<String>, Long> resultKStream = kStream
				.flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+"))) // split
				.map((k, v) -> new KeyValue<>(k, v.toLowerCase())) // converti en miniscule
				.filter((k, v) -> v.equals("a") || v.equals("b")) // recupere que "a" et "b"
				.groupBy((k, v) -> v) // grouper par "a" et "b"
				.windowedBy(TimeWindows.of(Duration.ofSeconds(5))) // durant toutes les 5 dernières secondes
				.count(Materialized.as("count-analytics")); // creation d'une vue "count-analytics"

		// convert et renvoi vers stream.  Ici en creer une key
		resultKStream.toStream().map((k, v) -> new KeyValue<>(k.window().startTime()+"-"+k.window().endTime()+"--"+k.key(), v)).to("resultTopic",
				Produced.with(Serdes.String(), Serdes.Long()));

		Topology topology = streamsBuilder.build();
		KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

		kafkaStreams.start();
	}

}
