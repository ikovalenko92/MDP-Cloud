package com.KafkaExample;

//takes JSON data from input stream and turns data into a string for kafka producer

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

public class JsonToString {
	public static String GetKafkaMessage(File jsonfile) {
		// Initialize this array to check later
		JsonArray array = null;

		// Use try-catch to avoid RunTimeError
		// when the file doesn't exist
		// or somehow the program cannot open the file
		  try {
		   InputStream fis = new FileInputStream(jsonfile);
			JsonReader jsonReader = Json.createReader(fis);
			array = jsonReader.readArray();
			jsonReader.close();
			fis.close();
		  } catch (Exception e) {}

		// This happens when program fails to open file
		  if (array == null) return null;

		// Get data from JSON array
		  JsonObject object = array.getJsonObject(0);
		  String TagName = object.getString("TagName");
		  String TagValue = object.getString("TagValue");
		  String Timestamp = object.getString("TimeStamp");
		  String KafkaMessage = Timestamp + " " + TagName + " " + TagValue;
		  return KafkaMessage;
	  }
   }

