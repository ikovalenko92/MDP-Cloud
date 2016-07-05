package com.mdp.example.KafkaExample;
 
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.*;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream; 

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Consumer {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "group1");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest"); 
        props.put("consumer.timeout.ms", "3000");

        
        ConsumerConfig conf = new ConsumerConfig(props);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(conf);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        String topic = "test_topic";
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        int num_msgs = 0;

        System.out.println("Start topic: " + topic);

        for (KafkaStream<byte[], byte[]> stream : streams){
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            try{
                while (it.hasNext()){
                    num_msgs++;
                    System.out.println("message: " + new String(it.next().message()));
                }
            }catch(ConsumerTimeoutException e){
                    System.out.println("Consumer time out, " + num_msgs + " messages has been printed");
                    consumer.shutdown();
                    System.exit(0);
            }
        }
        
        
        consumer.shutdown();   
    }
    

}