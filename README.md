# storm-plateform

An implementation of kNN algorithm on a real-time platform, namely a Apache Storm topology. 

### How to run the program
#### HBase part
- First, download HBase and run it using the command `bin/start-hbase.sh`. Verify with the `jps` command that you have
one running process called `HMaster`.
- Then, after importing all the dependencies with Maven, run the main class `otherClass.parseJSONtoDB`. It will
populate the database.

#### Kafka part
- Run the main class `kafka.MyKafkaCluster`. It will create a Kafka cluster and a topic.

#### Trident/Storm part
- When the two other processes are completely finished, you can either run `TopologyMain` or `TopologyBatchMain`.
The first will run a Storm topology and the second a Trident topology.
- Finally, you can run the `kafka.MyProducer` class.

### What the program do
In this version, the Storm and the Trident topologies work exactly the same way. They receive fake GPS positions from
Kafka, normalize these messages, then process the kNN algorithm to find the - how much? - nearest restaurants for each
position.