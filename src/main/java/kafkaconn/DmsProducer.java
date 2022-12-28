package kafkaconn;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DmsProducer<K, V> {
	public static final String CONFIG_PRODUCER_FILE_NAME = "dms.sdk.producer.properties";
	private Producer<K, V> producer;

	public DmsProducer() {
		try {
			Properties props=new Properties();
			props=loadFromClasspath(CONFIG_PRODUCER_FILE_NAME);
			producer=new KafkaProducer<K,V>(props);
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		
	}
	
	public DmsProducer(String path) {

		try {
			Properties props = new Properties();
			InputStream inputStr = new BufferedInputStream(new FileInputStream(path));
			props.load(inputStr);
			producer=new KafkaProducer<K,V>(props);
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return;
		}
	}

	public static ClassLoader getCurrentClassLoader() {
		ClassLoader classLoader=Thread.currentThread().getContextClassLoader();
		if(classLoader==null) {
			classLoader=DmsProducer.class.getClassLoader();
		}
		return classLoader;
	}
	
	public static Properties loadFromClasspath(String configFileName)throws IOException {
		Properties config=new Properties();
		ClassLoader classLoader=getCurrentClassLoader();
		
		List<URL> properties=new ArrayList<URL>();
		Enumeration<URL> propertyResources=classLoader.getResources(configFileName);
		while(propertyResources.hasMoreElements()) {
			properties.add(propertyResources.nextElement());
		}
		for(URL url:properties) {
			InputStream is=null;
			try {
				is=url.openStream();
				config.load(is);
			}
			finally {
				if(is!=null) {
					is.close();
					is=null;
				}
			}
		}
		return config;
	}
	
	public void produce(String topic,Integer partition,K key ,V data) {
		produce(topic,partition,key,data,null,(Callback)null);
	}
	
	public void produce(String topic,Integer partition,K key,V data,Long timestamp) {
		produce(topic,partition,key,data,timestamp,(Callback)null);
	}
	
	public void produce(String topic,Integer partition,K key,V data,Callback callback) {
		produce(topic,partition,key,data,null,callback);
	}
	
	public void produce(String topic,V data) {
		produce(topic,null,null,data,null,(Callback)null);
	}
	
	public void produce(String topic,Integer partition,K key,V data,Long timestamp,Callback callback) {
		ProducerRecord<K,V> kafkaRecord= timestamp==null? new ProducerRecord<K,V>(topic,partition,key,data):new ProducerRecord<K,V>(topic,partition,timestamp,key,data);
		produce(kafkaRecord,callback);
		
	}
	
	public void produce(ProducerRecord<K,V> kafkaRecord) {
		produce(kafkaRecord,(Callback)null);
	}
	
	public void produce(ProducerRecord<K,V>kafkaRecord,Callback callback) {
		producer.send(kafkaRecord,callback);
	}
	
	public void close() {
		producer.close();
	}
}
