package kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import kafka.Consts;



//consumer implementation
public class Consumer {
	private KafkaConsumer<String, String> consumer;
	private long key1_counter;
	private long key2_counter;
	
	
	public Consumer(int port) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:" + port);
		props.setProperty("group.id", "test");
		props.setProperty("enable.auto.commit", "false");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		this.consumer = new KafkaConsumer<String, String>(props);
		
		this.key1_counter = 0;
		this.key2_counter = 0;	
	}
	 
	
	public void consume(String[] topics) {
		consumer.subscribe(Arrays.asList(topics));
		//request massages from server 
		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
		
		for (ConsumerRecord<String, String> record : records) {
			//update keys counters
			if(record.key().equals("Key1")) key1_counter++;
			else if(record.key().equals("Key2")) key2_counter++;
		}	
	}
	
	
	public long getKey1_counter() {
		return key1_counter;
	}
	
	
	public long getKey2_counter() {
		return key2_counter;
	}
	
	
	public static void main(String[] args) {
		Consumer consumer = new Consumer(Consts.port);
		
		do {
			for(int i = 0; i < 1000; i++) 	
				consumer.consume(Consts.topics);
			
			System.out.println("Key1 counter : " + consumer.getKey1_counter());
			System.out.println("Key2 counter : " + consumer.getKey2_counter());
		}while(true);
	}

}