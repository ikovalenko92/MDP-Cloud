package com.KafkaExample;

//java 
import java.io.*;
import java.util.*;
import com.KafkaExample.JsonToString;

//topic
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import kafka.common.TopicExistsException;

//producer
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;
import scala.collection.JavaConversions;


//consumer packages
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.*;
import kafka.consumer.ConsumerTimeoutException;


public class Kafka_version_0821_compatiable
{
    public static void main(String[] args) throws Exception {

        //replace localhost with ip address to connect to remote host
        //10.242.7.19 this is the ip address of current server hosted by arc-ts for our kafka
        //zookeeper port= 2181, kafka port= 9092
        //all code must be compatible with kafka version 0.8.2.1 or lower
        
        String topic = "new"; 
        //change group id if you want consumer to read from the begining of the topic again
        // otherwise it will save its current position and only read new messages
        String group_id = "ybc";
        
        //comment create topic after first use for less output
        //exception is caught to stop program from terminating if not commeneted
        //only run producer or consumer one at a time or introduce a time delay for proper messaging 
        new_topic(topic);
        Run_producer(topic);
        Run_consumer(topic, group_id);
    }

    public static void  new_topic(String topic){
        try{
            //used to create topic
            ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
            // topic name, replication factor, replication factor, config properties
            AdminUtils.createTopic(zkClient, topic, 3, 1, new Properties());
        }
        catch (TopicExistsException e){
            System.out.println("Topic exists");
        }
    }

    public static void Run_producer(String topic){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        //change to whatever directory the json files are in
        String Dir = "C:/Users/tripplev/Desktop/complete/data"; //Directory with JSON files
        Producer<String, String> producer = new Producer<String, String>(config);
        List<KeyedMessage<String, String>> list = new ArrayList<KeyedMessage<String,String>>();
        File folder = new File(Dir); 
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) { //iterates through all the files in the directory
            //System.out.println(file);
            String[] KafkaMessages = JsonToString.GetKafkaMessage(file);
            int y = KafkaMessages.length;
            for(int x=0; x < y; x++){
                System.out.println(KafkaMessages[x]);
                KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, KafkaMessages[x]);
                producer.send(data);
            }
        }
        producer.close();
    }

    public static void Run_consumer(String topic,String group_id){
        Properties prop = new Properties();
        prop.put("zookeeper.connect", "localhost:2181");

        // if you want to consume fron the begining of the topic you must change group id value
        // only way because we are limited to kafka 0.8.2.1
        prop.put("group.id", group_id);
        //change group id to start reading from begining.
        //delete the line bellow if you never want to read from begining
        //in event of a crash change smallest to largest to read from the last available point
        prop.put("auto.offset.reset", "smallest");

        prop.put("zookeeper.session.timeout.ms", "500");
        prop.put("zookeeper.sync.time.ms", "250");
        prop.put("auto.commit.interval.ms", "1000");

        //times out consumer after 3 seconds if there are no messages to read
        prop.put("consumer.timeout.ms", "3000");

        ConsumerConnector consumer;
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(prop));
        
        Map<String, Integer> topicCount = new HashMap<>();
        topicCount.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
        for (final KafkaStream stream : streams) {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            try{
                while (it.hasNext()) {
                    System.out.println(topic + " : "+ new String(it.next().message()));
                }
            }
            catch(ConsumerTimeoutException e){
                System.out.println("Consumer timed out, no Messages to read");
                consumer.shutdown();
                System.exit(0);
            }
        }
        consumer.shutdown();   
    }
}