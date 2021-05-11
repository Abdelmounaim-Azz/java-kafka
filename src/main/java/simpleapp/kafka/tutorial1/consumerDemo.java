package simpleapp.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class consumerDemo {
    public static void main(String[] args) {
        String bootstrapServers="127.0.0.2:9092";
        String topic="first-topic";
        String groupId="kafka-demo";
        Logger logger= LoggerFactory.getLogger(consumerDemo.class.getName());
        Properties properties=new Properties();
        //Create consumer configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        //subscribe consumer to topic
        consumer.subscribe(Collections.singleton(topic));
        while(true){
           ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));
           for(ConsumerRecord<String,String> record:records){
               logger.info("Key:"+record.key()+", Value: "+record.value()+", Partition"+record.partition()+", Offset:"+
                       record.offset());
           }
        }
    }
}
