import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import otherClass.MyConstants;
import tridentFunctions.InputCompareToDBFunction;
import tridentFunctions.InputNormalizerFunction;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import tridentFunctions.ReducekNN;

/**
 * Created by sy306571 on 16/01/16.
 */
public class TopologyBatchMain {
    public static void main(String[] args) throws InterruptedException {

         /*Creation du spout Kafka pour Trident*/
        BrokerHosts zk = new ZkHosts("localhost:"+MyConstants.KAFKA_ZK_PORT);

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME);

        //spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        //L'initialisation d'autres parametres est necessaire pour faire du batching

        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        TridentTopology topology=new TridentTopology();

        topology.newStream("kafka-spout", spout)
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input","numPart"))
                .each(new Fields("input","numPart"), new InputCompareToDBFunction(1,95), new Fields("Nimporte1"))
                .each(new Fields("input"), new InputCompareToDBFunction(185,90), new Fields("Nimporte2"))
                .each(new Fields("input"), new InputCompareToDBFunction(0270,9), new Fields("Nimporte3"))
                .each(new Fields("input"), new InputCompareToDBFunction(356,90), new Fields("Nimporte4"))
                .aggregate(new Fields("knnParPartition"), new ReducekNN("Nimporte1","Nimporte3","Nimporte3"), new Fields("Finaloutput"));


        Config conf;
        conf = new Config();
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();

        System.err.println("START!!!!!!!!!!!!!!!!!!!!");

        cluster.submitTopology("Trident-Topology", conf, topology.build());
    }
}
