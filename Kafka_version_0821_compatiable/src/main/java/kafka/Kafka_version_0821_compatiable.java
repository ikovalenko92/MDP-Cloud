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
        //what to do if message integrity is comprimised. aka consumer returns false
        // what to do if message fails to send check with alec if there is a program to check if the server is up



        //replace localhost with ip address to connect to remote host
        //10.242.7.19 this is the ip address of current server hosted by arc-ts for our kafka
        //zookeeper port= 2181, kafka port= 9092
        //all code must be compatible with kafka version 0.8.2.1 or lower
        
        String topic = "new_topics"; 
        //change group id if you want consumer to read from the begining of the topic again
        // otherwise it will save its current position and only read new messages
        String group_id = "hellos";
        
        //comment create topic after first use for less output
        //exception is caught to stop program from terminating if not commeneted
        //only run producer or consumer one at a time or introduce a time delay for proper messaging 
        new_topic(topic);
        int [] hash_values = Run_producer(topic);

        //stalls program for 10seconds to ensure it was recieved by kafka server, before consumer runs
        try {
            Thread.sleep(10000);                 
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        
        boolean messages_recieved = Run_consumer(topic, group_id, hash_values);
        if(messages_recieved){
            System.out.println("Message(s) sent successfully");
        }
        else {
            System.out.println("Message(s) sent unsuccessfully");
        }
    }

    public static int [] double_size(int [] hash_values){
        int new_array [] = new int [hash_values.length*2];
        System.arraycopy(hash_values, 0, new_array, 0, hash_values.length);
        return new_array;
    }

    public static void  new_topic(String topic){
        try{
            //used to create topic
            ZkClient zkClient = new ZkClient("migsae-kafka.aura.arc-ts.umich.edu:2181/kafka", 10000, 10000, ZKStringSerializer$.MODULE$);
            // topic name, replication factor, replication factor, config properties
            AdminUtils.createTopic(zkClient, topic, 3, 1, new Properties());
        }
        catch (TopicExistsException e){
            System.out.println("Topic exists");
        }
    }

    public static int [] Run_producer(String topic){
        Properties props = new Properties();
        props.put("bootstrap.servers", "migsae-kafka.aura.arc-ts.umich.edu:9092");
        props.put("acks", "all");
        props.put("metadata.broker.list", "migsae-kafka.aura.arc-ts.umich.edu:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        //change to whatever directory the json files are in
        String Dir = "C:/Users/VRMILLING/Desktop/CLOUD DATA/Cloud-Big-data-Kafka_version_0821_compatiable/Kafka_version_0821_compatiable/data"; //Directory with JSON files
        Producer<String, String> producer = new Producer<String, String>(config);
        List<KeyedMessage<String, String>> list = new ArrayList<KeyedMessage<String,String>>();
        File folder = new File(Dir); 
        File[] listOfFiles = folder.listFiles();

        int [] hash_codes= new int[1000];
        //first spot keeps track of how many hashvalues are currently stored
        hash_codes[0]=1;

        for (File file : listOfFiles) { //iterates through all the files in the directory
            //System.out.println(file);
            String[] KafkaMessages = JsonToString.GetKafkaMessage(file);
            int y = KafkaMessages.length;
            if(hash_codes[0]+KafkaMessages.length >= hash_codes.length){
                hash_codes = double_size(hash_codes);
            }
            for(int x=0; x < y; x++){
                hash_codes[hash_codes[0]]= KafkaMessages[x].hashCode();
                System.out.println(KafkaMessages[x] + " " + hash_codes[hash_codes[0]]);
                KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, KafkaMessages[x]);
                producer.send(data);
                
                //counts how many hashcodes were created
                hash_codes[0]= hash_codes[0] + 1;
            }
        }
        producer.close();
        return hash_codes;
    }

    public static boolean Run_consumer(String topic,String group_id, int [] hash_codes){
        Properties prop = new Properties();
        // for(int x=0; x< 7; x++){
        //     System.out.println(hash_codes[x]);
        // }
        prop.put("zookeeper.connect", "migsae-kafka.aura.arc-ts.umich.edu:2181/kafka");

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
        int x= 1;
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
        for (final KafkaStream stream : streams) {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            try{
                while (it.hasNext()) {
                    String new_message =new String(it.next().message());
                    System.out.println(topic + " : "+ new_message + " "+ new_message.hashCode() + " " + hash_codes[x]); 
                    if(hash_codes[x] != new_message.hashCode()){
                        System.out.println("Message integrity comprimised");
                        return false;
                    }
                    x++;
                }
            }
            catch(ConsumerTimeoutException e){
                System.out.println("Consumer timed out, no Messages to read");
                consumer.shutdown();
                return true;
            }
        }
        consumer.shutdown(); 
        return true;  
    }
}