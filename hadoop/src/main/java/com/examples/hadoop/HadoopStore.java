package com.mdp.example.hadoop;

import java.io.*;
import java.util.*;
//import com.mdp.example.KafkaExample.KafkaDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HConstants; 

import parquet.Log;
import parquet.example.data.Group;
import parquet.column.ParquetProperties;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.Type;



public class HadoopStore{

	public static Path root = new Path("hdfs://localhost:9000/parquet/");

	public static Configuration conf = new Configuration();

	public static MessageType schema = MessageTypeParser.parseMessageType( 
            " message { " + 
                    "required string tagname;" + 
                    "required string timestamp;" + 
                    "required int tagvalue;" + 
                    " }" 
    ); 

    public static SimpleGroupFactory simplegroupfactory = new SimpleGroupFactory(schema);

    public static Map<String, List<String>> getTables(){
        Map<String, List<String>> tables = new HashMap<>();
        List<String> row1 = new ArrayList();
        row1.add("/Date(1464125927350-0400)/ 1800");
        tables.put("::[New_Shortcut]ABBLoopVFD_N046:I.OutputFreq", row1);

        List<String> row2 = new ArrayList();
        row2.add("/Date(1464125930350-0400)/ 1079");
        tables.put("::[New_Shortcut]FanucLoopVFD_N045:I.OutputVoltage", row2);

        return tables;
    }    

    public static void main(String[] args) throws Exception
    {
    	Map<String, List<String>> tables = getTables();
        if (tables.empty){
            System.out.println("empty table");
            exit(1);
        }
        Set<String> keys = tables.keySet();
        for (String key : keys){
            List<String> records = tables.get(key);
            int size = records.size();
            Map<String, String> meta= new HashMap<>();
            meta.put(HConstants.START_KEY, genRowKey("%20d", key+1));
            meta.put(HConstants.END_KEY, genRowKey("%20d", key+size));

            GroupWriteSupport.setSchema(schema, conf);

            ParquetWriter<Group> writer = new ParquetWriter<Group>(
                new Path(root, key),
                new GroupWriteSupport(meta),
                CompressionCodecName.SNAPPY,
                1024,
                1024,
                512,
                true,
                false,
                ParquetProperties.WriteVersion.PARQUET_1_0,
                conf);
            int row = 0;
            for (String record : records){
                row++;
                String[] parts = record.split("\\s+");
                String value = parts[0];
                String timestamp = parts[1];
                int tagvalue = Integer.parseInt(value);
                Group group = simplegroupfactory.newGroup().append("tagname", key)
                    .append("tagvalue", tagvalue)
                    .append("timestamp", timestamp);

                writer.write(group);
            }
            writer.close();
            
        }
    }	
}

