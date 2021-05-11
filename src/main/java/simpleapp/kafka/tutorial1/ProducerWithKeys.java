package simpleapp.kafka.tutorial1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {
    public static void main(String[] args) {

        Logger logger= LoggerFactory.getLogger(ProducerWithKeys.class);
        String bootstrapServers="127.0.0.2:9092";
        //Create Producer Properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        for(int i=0;i<5;i++){
            String topic="first-topic";
            String value="data produced"+Integer.toString(i);
            String key="MyKey"+Integer.toString(i);
        //Create producer record
        ProducerRecord<String,String> record=new ProducerRecord<String,String>(topic,key,value);
        logger.info("Key: "+key);
        //Send data -async
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e== null){
                    logger.info("Received new metadata.\n"+"Topic:"+recordMetadata.topic()+"\n"+"Partition:"+recordMetadata.partition()+"\n"+"Offset:"+
                            recordMetadata.offset()+"\n"+"Timestamp:"+recordMetadata.timestamp());
                }
                else{
                    logger.error("Error while producing",e);
                }
            }
        });
    }
        producer.flush();
        producer.close();
    }
}
