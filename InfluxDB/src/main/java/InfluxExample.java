package com.mdp.example.InfluxDatabase;

import java.io.*;
import java.lang.*;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;

import java.util.*;
import com.mdp.example.InfluxDatabase.JsonToString;

import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

public class InfluxExample {
    public static void main(String[] args){
    	InfluxDB influxDB = InfluxDBFactory.connect("https://migsae-influx.arc-ts.umich.edu:8086", "cloud_data", "2016SummerProj");
        String dbName = "test";
        String measurementName = "OldValues";

        BatchPoints batchPoints = BatchPoints
                    .database(dbName)
                    .retentionPolicy("default")
                    .consistency(ConsistencyLevel.ALL)
                    .build();     

        int j=10;
        int totalsum=0;
        int totalnum=0;
        int previoustotal=0;
        int next_remove_index=0;
        int [] numbercount= new int[j];
        for(int x=0;x<j;x++){
            numbercount[x]=0;
        }
        Boolean start=false;

        String Dir = "/home/venkatv/InfluxDB/influxdb/data";
        File folder = new File(Dir); 
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) { //iterates through all the files in the directory
            
            String[] KafkaMessages = JsonToString.GetKafkaMessage(file);
            int y = KafkaMessages.length;
            for(int x=0; x < y; x++){
                String to_add_point= KafkaMessages[x];
                String parts[]=to_add_point.split(" ");
                String tag= parts[0];
                String timeStamp=parts[1];
                int tagValue = Integer.parseInt(parts[2]);
                long time_units=System.currentTimeMillis();
                Point point1=Point.measurement(measurementName)
                        .time(time_units, TimeUnit.MILLISECONDS)
                        .addField("tagName", tag)
                        .addField("timeStamp", timeStamp)
                        .addField("tagValue", tagValue)
                        .build();
                batchPoints.point(point1);
                influxDB.write(batchPoints);

                if(tag.equals("::[New_Shortcut]FanucLoopVFD_N045:I.OutputVoltage")){
                    previoustotal= totalsum;
                    totalsum= totalsum + tagValue;
                    totalnum = totalnum +1;
                    
                    if(totalnum<j){
                        numbercount[totalnum-1]=tagValue;
                    }
                    if(totalnum>j){
                        totalsum= totalsum-numbercount[next_remove_index];
                        numbercount[next_remove_index]= tagValue;
                        next_remove_index= next_remove_index +1;
                        if(next_remove_index==j){
                            next_remove_index=0;
                        }
                        totalnum=j;
                        start=true;

                    }
                    System.out.println("total:"+ previoustotal+ "prev:"+ totalsum +"avg:"+ totalsum/totalnum);
                    if(totalnum==j &&start){
                        if(((totalsum/j)>= (1.25*previoustotal/j)) || ((totalsum/j)<= (.75*previoustotal/j)) ){
                            Point error=Point.measurement("loop_warnings")
                                .time(time_units, TimeUnit.MILLISECONDS)
                                .addField("error", -1)
                                .build();
                            batchPoints.point(error);
                            System.out.println("avg difference");
                        }

                    }
                    

                }
            }
        }
        influxDB.write(batchPoints);
    }
}
