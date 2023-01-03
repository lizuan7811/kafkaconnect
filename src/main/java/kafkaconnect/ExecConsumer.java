package kafkaconnect;

import java.util.Arrays;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ExecConsumer {

	public void doConsumer()throws Exception{
		DmsConsumer consumer=new DmsConsumer();
		
		consumer.consume(Arrays.asList("filebeat-kafka-elastic"));
		System.out.println(">>>>>>>>>>>>>>>>>consume");
		try {
			for(int i=0;i<10;i++) {
				System.out.println(">>>>>>>>>>>>>>>>>for loop");

//				ConsumerRecords<Object, Object> records=consumer.poll(Duration.create(1000, ""));
				ConsumerRecords<Object, Object> records=consumer.poll(1000);
				System.out.println("the numbers of topic:"+records.count());
				records.forEach(obj->System.out.println(obj.toString()));
			}
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			consumer.close();
		}
		
	}
	
}
