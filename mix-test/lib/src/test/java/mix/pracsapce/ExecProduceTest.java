package mix.pracsapce;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.Records;
import org.junit.jupiter.api.Test;

public class ExecProduceTest {
	
	
	@Test
	public void testProducer() throws Exception{
		DmsProducer<String,String> producer=new DmsProducer<String,String>();
		int partition=0;
		try {
			for(int i=0;i<10;i++) {
				String key=null;
				String datas="The msg is: "+i;
				producer.produce("topic-0",partition,key,datas,new Callback() {
					public void onCompletion(RecordMetadata metadata,Exception exception) {
						if(Objects.isNull(exception)) {
							exception.printStackTrace();
							return;
						}
						System.out.println("Produce msg Complete!");
					}
				});
				System.out.println("Produce Msg :"+datas);
			}
		}catch(Exception ioe)
		{
			ioe.printStackTrace();
		}finally{
			producer.close();
		}
		
	}
	
	@Test
	public void testComsumer()throws Exception{
		
		DmsConsumer consumer =new DmsConsumer();
		consumer.consume(Arrays.asList("topic-0"));
		try {
			for(int i=0;i<10;i++) {
				ConsumerRecords<Object,Object>records=consumer.poll(Duration.ofMillis(System.currentTimeMillis()));
				records.iterator().forEachRemaining(System.out::println);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		finally {
			consumer.close();
		}
	}
}
