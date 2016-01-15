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
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;

import java.util.Properties;


public class TopologyMain {

        public static void main(String[] args) throws InterruptedException {

                /*Creation du spout Kafka pour Trident*/
                BrokerHosts zk = new ZkHosts("localhost"); //Pas besoin de specifier le port ?
                //ZkHosts zkHosts = new ZkHosts("localhost:2181");
                /*GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
                partitionInfo.addPartition(0,new Broker("localhost:9092"));
                partitionInfo.addPartition(1,new Broker("localhost:9092"));
                partitionInfo.addPartition(2,new Broker("localhost:9092"));
                partitionInfo.addPartition(3,new Broker("localhost:9092"));
                StaticHosts staticHosts = new StaticHosts(partitionInfo);*/

                //Un troisieme parametre est-il necessaire ?
                TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME, "");

                spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
                //L'initialisation d'autres parametres est sans doute necessaire

                //Est-ce le bon type de spout ?
                OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

                /*String topic = MyConstants.TOPIC_NAME;
                String consumer_group_id = "id7";
                SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic, "", consumer_group_id);

                kafkaConfig.startOffsetTime = OffsetRequest.EarliestTime();
                kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());


                KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);*/

                TridentTopology topology=new TridentTopology();
                topology.newStream("kafka-spout",spout);


                /*builder.setSpout("KafkaSpout", kafkaSpout, MyConstants.NUM_PARTITIONS);
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

                System.err.println("END!!!!!!!!!!!!!!!!!!!!");*/
	}
}
