package mix.pracsapce;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class DmsConsumer {
	
	public static final String CONFIG_CONSUMER_FILE_NAME="dms.sdk.comsumer.properties";
	
	private KafkaConsumer<Object,Object>consumer;
	
	public DmsConsumer() {
		
		Properties props=new Properties();
		try {
			props=loadFromClasspath(CONFIG_CONSUMER_FILE_NAME);
		}catch(Exception ex) {
			ex.printStackTrace();
			return ;
		}
		consumer=new KafkaConsumer<Object,Object>(props);
	}
	
	
	public DmsConsumer(String path) {
		
		Properties props=new Properties();
		try {
			
		}catch(Exception ex) {
			ex.printStackTrace();
			return ;
		}
		consumer=new KafkaConsumer<Object,Object>(props);
	}

	public void consume(List<String> topics) {
		consumer.subscribe(topics);
	}
	
	public ConsumerRecords<Object,Object> poll(Duration timeout){
		
		return consumer.poll(timeout);
	}
	
	public void close() {
		consumer.close();
	}
	
	public static ClassLoader getCurrentClassLoader() {
		ClassLoader classLoader=Thread.currentThread().getContextClassLoader();
		
		if(Objects.isNull(classLoader)) {
			classLoader=DmsConsumer.class.getClassLoader();
		}
		return classLoader;
	}

	public static Properties loadFromClasspath(String configFileName)throws IOException{
		
		ClassLoader classLoader=getCurrentClassLoader();
		
		Properties config=new Properties();
		
		List<URL>properties= new ArrayList<URL>();
		
		Enumeration<URL>propertyResources=classLoader.getResources(configFileName);
		
		propertyResources.asIterator().forEachRemaining(url->{
			properties.add(url);
		});
		
		properties.stream().forEach(url->{
			InputStream is=null;
			try {
				config.load(is);
			} catch (IOException e) {
				e.printStackTrace();
			}finally{
				if(is!=null) {
					try {
						is.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		});
		return config;
	}
}
