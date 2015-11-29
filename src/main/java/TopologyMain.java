import backtype.storm.spout.SchemeAsMultiScheme;
import kafka.api.OffsetRequest;
import spouts.InputReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.InputCompareToDB;
import bolts.InputNormalizer;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


public class TopologyMain {
        public static void main(String[] args) throws InterruptedException {
                TopologyBuilder builder=new TopologyBuilder();

                ZkHosts zkHosts = new ZkHosts("localhost:2181");
                String topic = "gps";
                String consumer_group_id = "id7";
                SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic, "", consumer_group_id);

                kafkaConfig.startOffsetTime = OffsetRequest.EarliestTime();
                kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

                KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
                builder.setSpout("KafkaSpout", kafkaSpout);
                builder.setBolt("input-normalizer", new InputNormalizer()).shuffleGrouping("KafkaSpout");
                builder.setBolt("input-compareToDB", new InputCompareToDB(), 1).fieldsGrouping("input-normalizer", new Fields("inputcoord"));

                Config conf;
                conf = new Config();
                //conf.put("inputFile", args[0]);
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
