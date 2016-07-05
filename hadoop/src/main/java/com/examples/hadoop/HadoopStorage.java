package com.mdp.example.hadoop;

import java.io.*;
import java.util.*;
import com.mdp.example.KafkaExample;

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



public class HadoopStorage extends Configured implements Tool {

	public static Path root = new Path("hdfs://localhost:9000/parquet/");

	public static Configuration conf = new Configuration();

	public static MessageType schema = MessageTypeParser.parseMessageType( 
            " message { " + 
                    "required string tag;" + 
                    "required string timestamp;" + 
                    "required int value;" + 
                    " }" 
    ); 

    public static SimpleGroupFactory simplegroupfactory = new SimpleGroupFactory(schema);

    public class Map extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Void, Group>.Context context) throws IOException, InterruptedException
        {
        	//convert to parquet
        	
        	Group group = factory.newGroup()
        		.append("tagvalue", tagvalue)
        		.append("timestamp", timestamp);
            context.write(contents, key);
        }
    }


    public int run(String[] args) throws Exception
    {

    	Map<String, List<String>> tables = getTables();
    	if (tables.empty){
        	System.out.println("empty table");
        	exit(1);
    	}
    	Set<String> keys = tables.keySet();

        Job job = new Job(getConf());

        job.setJarByClass(getClass());

        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(ExampleInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
    	try{
        	int res = ToolRunner.run(conf, new HadoopStorage());

        	System.exit(res);
        }catch (Exception e){
        	e.printStackTrace();
        	System.exit(255);
        }
    }	


    //     public class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>
    // {
    //     public void reduce(Text key, Iterable<LongWritable> vals, Context context) throws IOException, InterruptedException
    //     {
    //         for(LongWritable tmp:vals){
    //             try{
				// 	String hdfsPath = "hdfs://localhost:9000/";

				// 	Configuration config = new Configuration();
				// 	config.set("fs.default.name", hdfsPath);

				// 	//FileSystem fs = FileSystem.get(conf);
				// 	FileSystem fs = FileSystem.getLocal(config);

				// 	Set<String> keyset = tables.keySet();
				// 	String dst = "hdfs://localhost:9000/";
				// 	for (String filename : keyset){
				// 		//create destination path
				// 		filename = filename + ".dat";
				// 		if (dst.charAt(dst.length() - 1) != '/'){
				// 			dst = dst + '/' + filename;
				// 		}else{
				// 			dst = dst + filename;
				// 		}

				// 		//check file exist or not
				// 		Path path = new Path(dst);
				// 		FSDataOutputStream out;
				// 		if (fs.exists(path)){
				// 			out = fs.append(path);
				// 		}else{
				// 			out = fs.create(path);
				// 		}

				// 		List<String> val = 	tables.get(filename);
				// 		for (String lines : val){
				// 			byte[] b = lines.getBytes();
				// 			out.write(b);
				// 		}
				// 		out.close();

				// 		FileStatus filestatus = fs.getFileStatus(path);
				// 		System.out.println(filename + " length: " + filestatus.getLen());
				// 		System.out.println(filename + " Modification time: " + filestatus.getModificationTime());
				// 	}
						
				// 	fs.close();
				// }catch (IOException e){
				// 	e.printStackTrace();
				// }
    //         }
    //     }
    // }

}