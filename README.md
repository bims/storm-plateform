# Trident platform (Apache Storm technology)

An implementation of kNN algorithm on a real-time platform, namely a Trident topology using Apache Storm.

### How to use the program
#### HBase part
- First, download HBase, go to its repository and run the command `bin/start-hbase.sh`. Verify with the `jps` 
command that you have one running process called `HMaster`.
- Then, after importing all the dependencies with Maven, run the main class `hbase.parseJSONtoDB`. It will
create and populate a database. If you want to process z-Value based algorithms, you need to populate 
the database with sorted items along with their z-Values. To do so, give the number 1 as argument to the program. 
/!\ Notice that a z-Value based database will not work with the BNLJ topologies. You can re-run the main class if you
want to recreate the database.

#### Kafka part
- Run the main class `kafka.MyKafkaCluster`. It will create a Kafka cluster and a topic.
- Run the main class `kafka.MyProducer` with as argument the number of messages you want to put in the topic.
By default, this number is 20. Unfortunately, to do batch processing with our program, you need to put all the 
messages once and for all in the topic.

#### Trident/Storm part
- When the two previous processes are completely finished, you can either run:
    * a Storm topology: 
        * `TopologyMain` will process the BNLJ algorithm without any partitioning or pre-processing,
    * a Trident topology:
        * `TopologyBNLJMain` will process the BNLJ algorithm with partitioning but insufficient parallelism,
        * `TopologyBNLJ2Main` will process BNLJ with a better parallelism than `TopologyBNLJMain` but not optimized,
        * `TopologyBNLJ3Main` will process BNLJ with enough parallelism,
        * `TopologyZValueMain` will process the zkNN Join algorithm with enough parallelism,
        * `TopologyQuickZkNNMain` will process the Quick zkNN algorithm with enough parallelism.
- Each one of these main classes need two arguments: the first to give the number of partitions (for R and S) and
the second to give the value of k. By default, the number of partitions is 4 and k is 11.