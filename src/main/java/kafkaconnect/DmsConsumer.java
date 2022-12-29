package kafkaconnect;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import scala.concurrent.duration.Duration;

public class DmsConsumer {
	
	public static final String CONFIG_CONSUMER_FILE_NAME="dms.sdk.consumer.properties";
	
	private KafkaConsumer<Object,Object>consumer;
	
	public DmsConsumer() {
//		try {
//			Properties props=new Properties();
//			props=loadFromClasspath(CONFIG_CONSUMER_FILE_NAME);
//			consumer=new KafkaConsumer<Object,Object>(props);
//		}catch(IOException ioe) {
//			ioe.printStackTrace();
//			return ;
//		}
	
	}
	
	public DmsConsumer(String path) {
		try {
			Properties props=new Properties();
			InputStream inputStr=new BufferedInputStream(new FileInputStream(path));
			props.load(inputStr);
			consumer=new KafkaConsumer<Object,Object>(props);
		}catch(IOException ioe) {
			ioe.printStackTrace();
			return;
		}
	}
	
	public void consume(List topics) {
		consumer.subscribe(topics);
	}
	
//	public ConsumerRecords<Object,Object>poll(long timeout){
////		return consumer.poll(Duration.fromNanos(timeout));
//	}
	
	
}
