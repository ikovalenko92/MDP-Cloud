package com.mdp.example.KafkaExample;

import java.util.*;
import java.io.File;

import com.mdp.example.KafkaExample.JsonToString;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;
import scala.collection.JavaConversions;

public class ProducerExample {
	public static void main(String[] args) {
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


		//String Dir = "/home/mschwieb/SampleCloudData"; //Directory with JSON files
		String Dir = "/Users/tanxiaoho/documents/Cloud/Cloud-Big-data/example/data";

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
				   	ProducerRecord<String, String> record = new ProducerRecord<String, String>("test_topic", KafkaMessage);
				   	producer.send(record, new myCallback(KafkaMessage));
				}
			}
			//producer.send(JavaConversions.asScalaBuffer(list));
		}
		producer.close();
	}
}

class myCallback implements Callback{
	private String message;

	public myCallback(String m){
		this.message = m;
	}

	public void onCompletion(RecordMetadata metadata, Exception e){
		if (e != null)
			e.printStackTrace();
		System.out.println(message);
		//System.out.println("The offset of the message " + message + " we just sent is: " + metadata.offset);
	}
}