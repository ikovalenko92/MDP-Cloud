package com.mdp.example.KafkaExample;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.io.File;

import com.mdp.example.KafkaExample.JsonToString;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;
import scala.collection.JavaConversions;

public class ProducerExample {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("metadata.broker.list", "localhost:9092");
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);

		String Dir = "/home/mschwieb/SampleCloudData"; //Directory with JSON files

		Producer<String, String> producer = new Producer<String, String>(config);
      List<KeyedMessage<String, String>> list = new ArrayList<KeyedMessage<String,String>>();
      File folder = new File(Dir); 
      File[] listOfFiles = folder.listFiles();

      for (File file : listOfFiles) { //iterates through all the files in the directory
        //System.out.println(file);
		  String KafkaMessage = JsonToString.GetKafkaMessage(file);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("test_topic", KafkaMessage);
	     list.add(data);
		  producer.send(JavaConversions.asScalaBuffer(list));
		}
		producer.close();
	}
}
