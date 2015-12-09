import backtype.storm.spout.SchemeAsMultiScheme;
import bolts.TestKafka;
import kafka.api.OffsetRequest;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.InputCompareToDB;
import bolts.InputNormalizer;
import otherClass.MyConstants;
import spouts.InputReader;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.Properties;


public class TopologyMain {

        public static void main(String[] args) throws InterruptedException {
                TopologyBuilder builder=new TopologyBuilder();

                ZkHosts zkHosts = new ZkHosts("localhost:2181");
                /*GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
                partitionInfo.addPartition(0,new Broker("localhost:9092"));
                partitionInfo.addPartition(1,new Broker("localhost:9092"));
                partitionInfo.addPartition(2,new Broker("localhost:9092"));
                partitionInfo.addPartition(3,new Broker("localhost:9092"));
                StaticHosts staticHosts = new StaticHosts(partitionInfo);*/

                String topic = MyConstants.TOPIC_NAME;
                String consumer_group_id = "id7";
                SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic, "", consumer_group_id);

                kafkaConfig.startOffsetTime = OffsetRequest.EarliestTime();
                kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

                KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
                builder.setSpout("KafkaSpout", kafkaSpout, MyConstants.NUM_PARTITIONS);
                //builder.setBolt("test-kafka",new TestKafka()).shuffleGrouping("KafkaSpout");
                builder.setBolt("input-normalizer", new InputNormalizer()).shuffleGrouping("KafkaSpout");
                builder.setBolt("input-compareToDB", new InputCompareToDB(), 1).fieldsGrouping("input-normalizer", new Fields("inputcoord"));

                Config conf;
                conf = new Config();
                conf.setDebug(false);

                conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
                LocalCluster cluster = new LocalCluster();

                System.err.println("START!!!!!!!!!!!!!!!!!!!!");

                cluster.submitTopology("Getting-Started-Topology", conf, builder.createTopology());

                System.err.println("Thread.sleep(4000)!!!!!!!!!!!!!!!!!!!!");

                Thread.sleep(4000);
                cluster.shutdown();

                System.err.println("END!!!!!!!!!!!!!!!!!!!!");
	}
}
