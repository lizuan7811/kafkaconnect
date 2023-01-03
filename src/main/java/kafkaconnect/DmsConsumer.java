package kafkaconnect;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import scala.concurrent.duration.Duration;

public class DmsConsumer {

	public static final String CONFIG_CONSUMER_FILE_NAME = "dms.sdk.consumer.properties";

	private KafkaConsumer<Object, Object> consumer;

	public DmsConsumer() {
		Properties props = new Properties();
		props = loadFromClasspath(CONFIG_CONSUMER_FILE_NAME);
		consumer = new KafkaConsumer<Object, Object>(props);
	}

	public DmsConsumer(String path) {
		try {
			Properties props = new Properties();
			InputStream inputStr = new BufferedInputStream(new FileInputStream(path));
			props.load(inputStr);
			consumer = new KafkaConsumer<Object, Object>(props);
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return;
		}
	}

	public void consume(List<String> topics) {
		
		consumer.subscribe(topics);
	}

	public ConsumerRecords<Object, Object> poll(final long timeout) {
		System.out.println(">>>POLL");
		return consumer.poll(timeout);
	}

	public void close() {
		consumer.close();
	}

	public static ClassLoader getCurrentClassLoader() {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		return Objects.isNull(classLoader) ? DmsConsumer.class.getClassLoader() : classLoader;
	}

	public static Properties loadFromClasspath(String configFileName) {
		Properties config = new Properties();
		try {
			ClassLoader classLoader = getCurrentClassLoader();
			
			Enumeration<URL> propertyResources = classLoader.getResources(configFileName);
			
			propertyResources.asIterator().forEachRemaining(url -> {
				try (InputStream inputStr = url.openStream()) {
					config.load(inputStr);
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		return config;
	}

}
