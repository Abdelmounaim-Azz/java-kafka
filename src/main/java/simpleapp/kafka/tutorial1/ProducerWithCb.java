package simpleapp.kafka.tutorial1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
public class ProducerWithCb {
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ProducerWithCb.class);
        String bootstrapServers="127.0.0.2:9092";
        //Create Producer Properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //Create producer record
        ProducerRecord<String,String> record=new ProducerRecord<String,String>("first-topic","data produced");
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
        producer.flush();
        producer.close();
    }
}
