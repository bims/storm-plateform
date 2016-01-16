import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import bolts.InputNormalizerFunction;
import otherClass.MyConstants;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;

/**
 * Created by sy306571 on 16/01/16.
 */
public class TopologyBatchMain {
    public static void main(String[] args) throws InterruptedException {

         /*Creation du spout Kafka pour Trident*/
        BrokerHosts zk = new ZkHosts("localhost"); //Pas besoin de specifier le port

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME);

        //spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme()); //Trouver le field approprie
        //L'initialisation d'autres parametres est necessaire pour faire du batching

        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        TridentTopology topology=new TridentTopology();
        topology.newStream("kafka-spout", spout)
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("off"));

        Config conf;
        conf = new Config();
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();

        System.err.println("START!!!!!!!!!!!!!!!!!!!!");

        cluster.submitTopology("Trident-Topology", conf, topology.build());
    }
}
