To run the code you must have a flux account with hadoop access and a queue for the hadoop cluster.
To apply for an account use this form: https://weblogin.umich.edu/?cosign-shibboleth.umich.edu&https://shibboleth.umich.edu/idp/Authn/UWLogin?conversation=e2s1

This code was built using the below github repo
https://github.com/dibbhatt/kafka-spark-consumer/tree/928fe9eb7d2fefbdd08a9127baa6d3880beee1ff

To run this code download and load maven into flux under the home directory and copy these files to flux.
Cd into the folder and run "mvn install".
Then create a jar with the Consumer.java example in the client folder.

For explanations of the driver code visit this link and scroll to dstreams:
https://spark.apache.org/docs/1.6.2/streaming-programming-guide.html

Below is an example of how to submit a job to the queue:
spark-submit --class Mainclassname --master yarn-client --executor-memory 3g --num-executors 5  --queue <your_queue> path/to/Mainclassname.jar

More examples can be found at:
http://arc-ts.umich.edu/hadoop-user-guide/


 
