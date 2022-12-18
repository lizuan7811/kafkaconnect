package mix.pracsapce;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.util.ObjectUtils;

import com.google.common.base.Objects;

import io.netty.util.internal.ObjectUtil;

public class DmsProducer<K, V> {

	public static final String CONFIG_PRODUCER_FILE_NAME = "dms.sdk.producer.properties";
	private Producer<K, V> producer;

	public DmsProducer() {
		Properties props = new Properties();
		try {
			props = loadFromClasspath(CONFIG_PRODUCER_FILE_NAME);
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return;
		}
		producer = new KafkaProducer<K, V>(props);

	}

	public DmsProducer(String path) {

		Properties props = new Properties();

		try {
			InputStream inputS = new BufferedInputStream(new FileInputStream(path));
			props.load(inputS);
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return;
		}
		producer = new KafkaProducer<K, V>(props);

	}

	private Properties loadFromClasspath(String classPath) throws IOException {
		ClassLoader classLoader = getCurrentClassLoader();
		Properties config = new Properties();
		List<URL> properties = new ArrayList<URL>();

		Enumeration<URL> propertyResources = classLoader.getResources(classPath);

		propertyResources.asIterator().forEachRemaining(url -> {
			properties.add(url);
		});

		properties.stream().forEach(url -> {
			InputStream is = null;
			try {
				is = url.openStream();
				config.load(is);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (java.util.Objects.nonNull(is)) {
					try {
						is.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					is = null;
				}
			}
		});

		return config;
	}

	private static ClassLoader getCurrentClassLoader() {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		if (java.util.Objects.isNull(classLoader)) {
			classLoader = DmsProducer.class.getClassLoader();
		}
		return classLoader;
	}
	
	public void produce(String topic,Integer partition,K key,V datas) {
		produce(topic,partition,key,datas,null,(Callback)null);
	}

	public void produce(String topic, Integer partition, K key, V datas, Long timeStamp) {

		produce(topic,partition,key,datas,timeStamp,(Callback)null);
	}

	public void produce(String topic ,Integer partition,K key,V datas,Callback callBack) {
		produce(topic,partition,key,datas,null,callBack);
	}
	
	public void produce(String topic,V datas) {
		produce(topic,null,null,datas,null,(Callback)null);
	}
	
	public void produce(String topic ,Integer partition,K key,V datas,Long timeStamp,Callback callBack) {
		ProducerRecord<K,V>kafkaRecord= java.util.Objects.isNull(timeStamp)?new ProducerRecord<K,V>(topic,partition,key,datas):new ProducerRecord<K,V>(topic,partition,timeStamp,key,datas);
		
		produce(kafkaRecord,callBack);
	}

	public void produce(ProducerRecord<K,V> kafkaRecord) {
		produce(kafkaRecord,(Callback)null);
	}
	
	public void produce(ProducerRecord<K,V> kafkaRecord,Callback callBack) {
		producer.send(kafkaRecord,callBack);
	}
	
	public void close() {
		producer.close();
	}
	
	
}
