package com.mdp.example.InfluxDatabase;

import java.io.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

public class InfluxExample {
    public static void main(String[] args){
    	InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "123456");
        String dbName = "test_db";
        String measurementName = "testValues";

        System.out.println("connection built");

        influxDB.createDatabase(dbName);

        System.out.println("created database");

        BatchPoints batchPoints = BatchPoints
                    .database(dbName)
                    // .tag("async", "true")
                    .retentionPolicy("default")
                    .consistency(ConsistencyLevel.ALL)
                    .build();

        Point point1 = Point.measurement(measurementName)
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .addField("tagName", "::[New_Shortcut]ABBLoopVFD_N046:I.OutputVoltage")
                        .addField("timeStamp", "/Date(1464125927350-0400)/")
                        .addField("tagValue", 34)
                        .build();
        batchPoints.point(point1);
            
        Point point2 = Point.measurement(measurementName)
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .addField("tagName", "::[New_Shortcut]FanucLoopVFD_N045:I.OutputFreq")
                        .addField("timeStamp", "/Date(1464125927350-0400)/")
                        .addField("tagValue", 1800)
                        .build();

        batchPoints.point(point2);
        influxDB.write(batchPoints);

        System.out.println("points written");

         Query query = new Query("SELECT * FROM "+measurementName, dbName);
         QueryResult res = influxDB.query(query);
         List<Result> list = res.getResults();
         for (Result l : list){
            if (l.hasError()){
                System.out.println(l.getError());
            }else{
                List<Series> series = l.getSeries();
                for (Series s : series){
                    System.out.println(s.getName());
                    List<String> columns = s.getColumns();
                    for (String col : columns){
                        System.out.print(col + "  ");
                    }
                    System.out.println();

                    List<List<Object>> values = s.getValues();
                    for (List<Object> vlist : values){
                        for (Object v : vlist){
                            System.out.print(v.toString() + "  ");
                        }
                        System.out.println();
                    }
                }
            }
         }

        //influxDB.deleteDatabase(dbName);
    }
}
