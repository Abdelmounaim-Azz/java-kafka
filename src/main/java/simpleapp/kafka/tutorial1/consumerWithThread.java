package simpleapp.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class consumerWithThread {
    public static void main(String[] args) throws InterruptedException {
        new consumerWithThread().run();
    }
    private consumerWithThread(){

    }
    private void run() throws InterruptedException {
        Logger logger= LoggerFactory.getLogger(simpleapp.kafka.tutorial1.consumerWithThread.class.getName());
        String bootstrapServers="127.0.0.2:9092";
        String topic="first-topic";
        String groupId="kafka-group";
        CountDownLatch latch=new CountDownLatch(1);
        logger.info("consumer is being created");
        Runnable myConsumerRunnable=new consumerRunnable(latch,topic,bootstrapServers,groupId);
        Thread myTh=new Thread(myConsumerRunnable);
        myTh.start();
        Runtime.getRuntime().addShutdownHook(new Thread(()=>{
            logger.info("caught shutdown hook");
            myConsumerRunnable.shutD();
        }));
        latch.await();

    }
    public class consumerRunnable implements Runnable{
        private Logger logger= LoggerFactory.getLogger(simpleapp.kafka.tutorial1.consumerWithThread.class.getName());
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        public consumerRunnable(CountDownLatch latch, String topic, String bootstrapServers, String groupId){
            this.latch=latch;
            Properties properties=new Properties();
            //Create consumer configs
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            //create consumer
            consumer=new KafkaConsumer<String, String>(properties);
            //subscribe consumer to topic
            consumer.subscribe(Collections.singleton(topic));
        }

        public void shutD(){
            consumer.wakeup();
        }
        @Override
        public void run() {
            try {
                while(true){
                    ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String,String> record:records){
                        logger.info("Key:"+record.key()+", Value: "+record.value()+", Partition"+record.partition()+", Offset:"+
                                record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown Signal");
            }
            finally {
                consumer.close();
                latch.countDown();//done with consumer
            }
        }

    }

}
