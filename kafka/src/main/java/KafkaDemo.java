package com.mdp.example.KafkaExample;

import java.util.*;
import java.io.*;
import java.lang.Exception;
import java.util.concurrent.TimeUnit;

import com.mdp.example.KafkaExample.JsonToString;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;
import scala.collection.JavaConversions;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.*;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream; 

//spark
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

//hadoop
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

//influxdb
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

public class KafkaDemo {

	private static Map<String, List<String>> tables;

	public static void main(String[] args) {
		System.out.println("Start Kafka Demo");
		String topic = "new_msgs";
		tables = new HashMap<String, List<String>>();

		run_Producer(topic);
		run_Consumer(topic);
		//store_HDFS();
	}

	public static void run_Producer(String topic){
		//.properties is a file extension for files mainly used in Java related technologies to store
		//the configurable parameters of an application. They can also be used for storing strings for
		//Internationalization and localization; these are known as Property Resource Bundles.
		// Properties props = new Properties();
		//  props.put("bootstrap.servers", "localhost:9092");
		// //props.put("bootstrap.servers", "10.242.7.19:9092");
		// props.put("acks", "all");
		//  props.put("metadata.broker.list", "localhost:9092");
		// //props.put("metadata.broker.list", "10.242.7.19:9092");
  //       props.put("serializer.class", "kafka.serializer.StringEncoder");
  //       props.put("request.required.acks", "1");
		//ProducerConfig config = new ProducerConfig(props);

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.required.acks", "1");


		//Directory with JSON files
		String Dir = "/Users/tanxiaoho/documents/Cloud/Cloud-Big-data/kafka/data";

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //List<KeyedMessage<String, String>> list = new ArrayList<KeyedMessage<String,String>>();
        File folder = new File(Dir); 
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) { //iterates through all the files in the directory
	        List<String> KafkaMessages = JsonToString.GetKafkaMessage(file);
	        if (KafkaMessages != null){
	        	for (String KafkaMessage : KafkaMessages){
			        //KeyedMessage<String, String> data = new KeyedMessage<String, String>("test_topic", KafkaMessage);
				    //list.add(data);
				   	//System.out.println(KafkaMessage);
				   	//ProducerRecord(String topic, K key, V value)
				   	ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, file.getName(), KafkaMessage);
				   	producer.send(record, new myProducerCallback(KafkaMessage));
				}
			}
			//producer.send(JavaConversions.asScalaBuffer(list));
		}
		System.out.println("Producer done");
		producer.close();
	}

	public static void run_Consumer(String topic){
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
        //String topic = "new_msgs";
        topicCountMap.put(topic, new Integer(1));

       /**
	   * This method is used to get a list of KafkaStreams, which are iterators over
	   * MessageAndMetadata objects from which you can obtain messages and their
	   * associated metadata (currently only topic).
	   *  Input: a map of <topic, #streams>
	   *  Output: a map of <topic, list of message streams>
	   */
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        int num_msgs = 0;

        System.out.println("Start topic: " + topic);

		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "123456");
        String dbName = "test_db";
        String measurementName = "testValues";

        System.out.println("connection built");

        influxDB.createDatabase(dbName);

        System.out.println("created database");

        // Flush every 2000 Points, at least every 100ms
		// influxDB.enableBatch(2000, 10, TimeUnit.MILLISECONDS);
        BatchPoints batchPoints = BatchPoints
                    .database(dbName)
                    // .tag("async", "true")
                    .retentionPolicy("default")
                    .consistency(ConsistencyLevel.ALL)
                    .build();
		int countpoint = 0;
        for (KafkaStream<byte[], byte[]> stream : streams){
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            try{
                while (it.hasNext()){
                    num_msgs++;
                    //System.out.println("message: " + new String(it.next().message()));
                    String m = new String(it.next().message());
                    String[] res = m.split(" ");
                    String key = res[0];
                    String value = res[1] + " " + res[2];
                    countpoint++;
					Point point = Point.measurement(measurementName)
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .addField("tagName", key)
                        .addField("timeStamp", res[1])
                        .addField("tagValue", res[2])
                        .build();
			        batchPoints.point(point);

                    if (tables.containsKey(key)){
                    	tables.get(key).add(value);
                    }else{
                    	List<String> rows = new ArrayList();
                    	rows.add(value);
                    	tables.put(key, rows);
                    }
                    //System.out.println(value);

                }
            }catch(ConsumerTimeoutException e){
                    System.out.println("Consumer time out, " + num_msgs + " messages has been printed");
					influxDB.write(batchPoints);

			         Query query = new Query("SELECT * FROM "+measurementName, dbName);
			         QueryResult res = influxDB.query(query);
			         List<Result> list = res.getResults();
			         int count = 0;

			         for (Result l : list){
			            if (l.hasError()){
			                System.out.println(l.getError());
			            }else{
			                List<Series> series = l.getSeries();
			                for (Series s : series){
			                    System.out.println(s.getName());
			                    List<String> columns = s.getColumns();
			                    for (String col : columns){
			                        System.out.print(col + "\t");
			                    }
			                    System.out.println();

			                    List<List<Object>> values = s.getValues();
			                    for (List<Object> vlist : values){
			                        for (Object v : vlist){
			                            System.out.print(v.toString() + "\t");
			                        }
			                        count++;
			                        System.out.println();
			                    }
			                    System.out.println("total num of msgs in series: " + count);
			                }
			            }
			         }
			        System.out.println("COUNT POINT = "+countpoint);

			        //drop series
			        Query dropSeries = new Query("DROP MEASUREMENT "+measurementName, dbName);
			        influxDB.query(dropSeries);

                    consumer.shutdown();
                    System.exit(0);
            }
        }
        //consumer.commitOffsets();
        consumer.shutdown();   
	}

	public static Map<String, List<String>> getTable(){
		return tables;
	}


	public static void store_HDFS(){
		try{
			String hdfsPath = "hdfs://localhost:9000/";

			Configuration conf = new Configuration();
			conf.set("fs.default.name", hdfsPath);

			//FileSystem fs = FileSystem.get(conf);
			FileSystem fs = FileSystem.getLocal(conf);

			Set<String> keyset = tables.keySet();
			String dst = "hdfs://localhost:9000/";
			for (String filename : keyset){
				//create destination path
				filename = filename + ".dat";
				if (dst.charAt(dst.length() - 1) != '/'){
					dst = dst + '/' + filename;
				}else{
					dst = dst + filename;
				}

				//check file exist or not
				Path path = new Path(dst);
				FSDataOutputStream out;
				if (fs.exists(path)){
					out = fs.append(path);
				}else{
					out = fs.create(path);
				}

				List<String> val = 	tables.get(filename);
				for (String lines : val){
					byte[] b = lines.getBytes();
					out.write(b);
				}
				out.close();

				FileStatus filestatus = fs.getFileStatus(path);
				System.out.println(filename + " length: " + filestatus.getLen());
				System.out.println(filename + " Modification time: " + filestatus.getModificationTime());
			}
				
			fs.close();
		}catch (IOException e){
			e.printStackTrace();
		}
	}

	public static void run_Spark(){
		//Spark wordcount
        SparkConf sparkconf = new SparkConf().setAppName("JavaMapping");
        JavaSparkContext context = new JavaSparkContext(sparkconf);
        JavaRDD<String> lines = context.textFile("Sample.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
		    	return Arrays.asList(s.split(" "));
	    	}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
		  	@Override
		  	public Tuple2<String, Integer> call(String s) {
		    	return new Tuple2<String, Integer>(s, 1);
		  	}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
		  	@Override
		 	public Integer call(Integer i1, Integer i2) {
		    	return i1 + i2;
		  	}
		});

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?,?> tuple : output) {
		  	System.out.println(tuple._1() + ": " + tuple._2());
		}
		context.stop();
	}

}

class myProducerCallback implements Callback{
	private String message;

	public myProducerCallback(String m){
		this.message = m;
	}

	public void onCompletion(RecordMetadata metadata, Exception e){
		if (e != null)
			e.printStackTrace();
		System.out.println(metadata.partition() + "  " + message);
		//System.out.println("The offset of the message " + message + " we just sent is: " + metadata.offset);
	}
}