package com.mdp.example.KafkaExample;
 
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
 
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;


 
public class ConsumerExample {
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;

    public class ConsumerTest implements Runnable {
        private KafkaStream m_stream;
        private int m_threadNumber;

        public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
            m_threadNumber = a_threadNumber;
            m_stream = a_stream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext())
                System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
            System.out.println("Shutting down Thread: " + m_threadNumber);
        }
    }
 
    public ConsumerExample(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }
 
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        // try {
        //     if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
        //         System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
        //     }
        // } catch (InterruptedException e) {
        //     System.out.println("Interrupted during shutdown, exiting uncleanly");
        // }
   }
 
    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);
 
        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber));
            threadNumber++;
        }
    }
 
    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
 
        return new ConsumerConfig(props);
    }
 
    public static void main(String[] args) {
        String zooKeeper = "localhost:2181";
        String groupId = "test-consumer";
        String topic = "test";
        int threads = 2;
 
        ConsumerExample example = new ConsumerExample(zooKeeper, groupId, topic);
        example.run(threads);
 
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
 
        }
        example.shutdown();
    }
}


// import org.apache.kafka.clients.consumer.KafkaConsumer;
// import org.apache.kafka.clients.consumer.ConsumerRecord;
// import org.apache.kafka.clients.consumer.ConsumerRecords;
// import org.apache.kafka.common.TopicPartition;

// // import kafka.consumer.ConsumerConfig;
// // import kafka.consumer.KafkaStream;
// // import kafka.javaapi.consumer.ConsumerConnector;
// //import org.apache.hadoop.fs.FileSystem;
 
// import java.util.HashMap;
// import java.util.List;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.Map;
// import java.util.Map.Entry;
// import java.util.Collections;
// import java.util.Properties;
// import java.io.File;
 

// public class ConsumerExample{

//     private static Map<TopicPartition, Long> process(Map<String, ConsumerRecords<String, String>> records) {
//          Map<TopicPartition, Long> processedOffsets = new HashMap<TopicPartition, Long>();
//          for(Map.Entry<String, ConsumerRecords<String, String>> recordMetadata : records.entrySet()) {
//               List<ConsumerRecord<String, String>> recordsPerTopic = recordMetadata.getValue().records();
//               for(int i = 0; i < recordsPerTopic.size();i++) {
//                   ConsumerRecord record = recordsPerTopic.get(i);
//                   // process record
//                   try {
//                       System.out.println(record.topic() + " : " + record.value());
//                       processedOffsets.put(record.topicAndPartition(), record.offset());
//                   } catch (Exception e) {
//                       e.printStackTrace();
//                   }               
//               }
//          }
//          return processedOffsets; 
//      }

//      public static void main(String[] args) {
//           Properties props = new Properties();
//           props.put("bootstrap.servers", "localhost:9092");
//           props.put("group.id", "test-consumer");
//           props.put("enable.auto.commit", "true");
//           props.put("auto.commit.interval.ms", "1000");
//           props.put("session.timeout.ms", "30000");
//           props.put("partition.assignment.strategy", "range");
//           props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//           props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//           KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//           consumer.subscribe("test_topic");
//           int numRecords = 0;
//           int commitInterval = 10;
//           boolean isRunning = true;
//           //List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
//           //final int minBatchSize = 200;
//           Map<TopicPartition, Long> consumedOffsets = new HashMap<TopicPartition, Long>();

//           System.out.println("start testing");

//           try{
//             while(isRunning){
//               Map<String, ConsumerRecords<String, String>> records = consumer.poll(1000);
//               if (records == null){
//                 System.out.println("empty records");
//                 break;
//               }

//                 //ConsumerRecords<String, String> records = consumer.poll(100);
//               Map<TopicPartition, Long> lastConsumedOffsets = process(records);
//               consumedOffsets.putAll(lastConsumedOffsets);
//               numRecords += records.size();
//               System.out.println(numRecords);
//                 // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
//               if(numRecords % commitInterval == 0){ 
//                     consumer.commit(consumedOffsets, true);
//               }
//             }
//           }finally {
//               //consumer.commit(true);
//               consumer.close();
//           }

//           // while (isRunning) {
//           //       Map<String, ConsumerRecords<String, String>> records = consumer.poll(100);
//           //       //ConsumerRecords<String, String> records = consumer.poll(100);
//           //       Map<TopicPartition, Long> lastConsumedOffsets = process(records);
//           //       consumedOffsets.putAll(lastConsumedOffsets);
//           //       numRecords += records.size();
//           //       System.out.println(numRecords);
//           //       // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
//           //       if(numRecords % commitInterval == 0){ 
//           //           consumer.commit(consumedOffsets, true);
//           //       }
//           // }
//           // consumer.commit(true);
//           // consumer.close();
//         // try {
//         //       while(true) {
//         //           ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);

//         //           for (TopicPartition partition : records.partitions()) {
//         //               List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
//         //               for (ConsumerRecord<String, String> record : partitionRecords) {
//         //                  //buffer.add(record);
//         //                  System.out.println(record.offset() + ": " + record.value());
//         //               }
//         //               long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//         //               consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
//         //           }
//         //       }
//         //   }finally {
//         //        consumer.close();
//         //   }

     
//      }
// }