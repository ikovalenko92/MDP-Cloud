This Code was built off of the below github repo
https://github.com/dibbhatt/kafka-spark-consumer/tree/928fe9eb7d2fefbdd08a9127baa6d3880beee1ff

# Running Spark Kafka Consumer

Let assume your Driver code is in xyz.jar which is built using the spark-kafka-consumer as dependency.

Launch this using spark-submit

./bin/spark-submit --class x.y.z.YourDriver --master spark://x.x.x.x:7077 --executor-memory 5G /<Path_To>/xyz.jar

This will start the Spark Receiver and Fetch Kafka Messages for every partition of the given topic and generates the DStream.

e.g. to Test Consumer provided in the package with your Kafka settings please modify it to point to your Kafka and use below command for spark submit 

./bin/spark-submit --class consumer.kafka.client.Consumer --master spark://x.x.x.x:7077 --executor-memory 5G /<Path_To>/kafka-spark-consumer-1.0.6-jar-with-dependencies.jar


 
