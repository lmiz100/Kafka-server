package kafka;

import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import kafka.Consts;



//producer implementation
public class Producer {
	
	private KafkaProducer<String, String> producer;
	private String[] topics;
	private String[] keys;
	private String[] values;
	
	
	public Producer(int port, String[] topics, String[] keys, String[] values) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:" + port);
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		this.producer = new KafkaProducer<String, String>(props);
		this.topics = topics;
		this.keys = keys;
		this.values = values;
		
	}
	
	
	public void produce() {
		Random rand = new Random();
		
		//sends up to 100 massages
		for (int i = 0; i < rand.nextInt(100); i++) {
			//choose random topic to send massage
			int addr = rand.nextInt(topics.length);
			
			producer.send(new ProducerRecord<String, String>(topics[addr], keys[addr], values[addr]));
		}
	}
	
	
	public void stop_produce() {
		producer.close();		
	}
	
	
	public static void main(String[] args) {
		Producer prod = new Producer(Consts.port, Consts.topics, Consts.keys, Consts.values);
		
		while(true) {
			prod.produce();
		}
	}

}
