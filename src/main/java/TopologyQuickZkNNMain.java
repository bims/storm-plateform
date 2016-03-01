import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import otherClass.MyConstants;
import otherClass.ZLimits;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import testTridentFunctions.testFunction;
import tridentFunctions.InputNormalizerFunction;
import tridentFunctions.zValue.*;

import java.math.BigInteger;
import java.util.HashMap;

public class TopologyQuickZkNNMain {
    public static void main(String[] args) throws InterruptedException {
        /*Creation du spout Kafka pour Trident*/
        BrokerHosts zk = new ZkHosts("localhost:"+ MyConstants.KAFKA_ZK_PORT);

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME);
        //spoutConf.fetchSizeBytes = 1000; //Sliding window

        int size = 380;
        int nbParts = 2;
        int k = 11;

        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        TridentTopology topology=new TridentTopology();

        HashMap<Integer, ZLimits> zLimits = new HashMap<>();
        zLimits.put(3, new ZLimits(new BigInteger("3"), new BigInteger("3")));
        SmartPartitionsFunction fct = new SmartPartitionsFunction(zLimits);

        topology.newStream("kafka-spout", spout)
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"))
                .each(new Fields("input"), new ZValueFunction(), new Fields("zValue"))
                .aggregate(new Fields("input", "zValue"), new SortAggregator(nbParts), new Fields("inputZValue", "numPartition"))
                .partitionBy(new Fields("numPartition"))
                .each(new Fields("inputZValue"), new testFunction("f"), new Fields("blabla"))
                .parallelismHint(nbParts)
                .shuffle()
                .each(new Fields("blabla"), new testFunction("Hey! "), new Fields("blabla2"))
                .parallelismHint(1);

       /* Stream firstStream = topology.newStream("kafka-spout", spout)
                .shuffle()
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"))
                .each(new Fields("input"), new ZValueFunction(), new Fields("zValue"))
                        //.parallelismHint(nbParts) //Ce n'est pas necessaire
                .each(new Fields("input", "zValue"), new SmartPartitionsFunction(zLimits), new Fields("inputZValue", "numPartition"))
                .each(new Fields("inputZValue", "numPartition"), new SmartZkNNFunction(0,0,0,0,zLimits), new Fields("res"));
                //.partitionBy(new Fields("numPartition"));

        /*List<Stream> streams = new ArrayList<>();
        for(int i=0; i<nbParts; i++){
            streams.add(firstStream.each(new Fields("inputZValue", "numPartition"), new PartitionFilter(i)).shuffle()
                    .each(new Fields("inputZValue", "numPartition"),
                            new ZkNNFunction(k, 0, size, nbParts, i, fct), new Fields("res"))
                    .parallelismHint(1));
        }*/

        Config conf;
        conf = new Config();
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10);
        LocalCluster cluster = new LocalCluster();

        System.err.println("START!!!!!!!!!!!!!!!!!!!!");

        cluster.submitTopology("Trident-Topology", conf, topology.build());

    }
}