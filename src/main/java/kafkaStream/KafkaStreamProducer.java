package kafkaStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaStreamProducer {

	String message;
	Random random = new Random();

	public static void main(String[] args) {

		new KafkaStreamProducer().start();

	}

	private void start() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "stream-producer-1");

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

		List<Character> charcaters = new ArrayList<Character>();
		for (char c = 'A'; c < 'Z'; c++) {
			charcaters.add(c);

		}

		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {

			message = "";

			for (int i = 0; i < 10; i++) {
				// creer une chaine avec des espaces entre les caractères.
				message += " " + charcaters.get(random.nextInt(charcaters.size()));
			}

			ProducerRecord<String, String> producerRecord = new ProducerRecord("bdccTopic", null, message);
			kafkaProducer.send(producerRecord ,new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e != null) {
                       e.printStackTrace();
                    } else {
                       System.out.println("offset: " + metadata.offset() + " partition: "+ metadata.partition() + " topic: " + metadata.topic() + " key: " + null +" message: " + message );
                    }
                }
            });


		}, 1000, 1000, TimeUnit.MILLISECONDS);
	}

}
