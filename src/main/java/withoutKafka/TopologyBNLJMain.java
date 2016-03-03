package withoutKafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import otherClass.HBaseDB;
import otherClass.MyConstants;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import tridentFunctions.InputCompareToDBFunction;
import tridentFunctions.InputNormalizerFunction;
import tridentFunctions.ReducekNNFunction;

import java.util.ArrayList;
import java.util.List;

public class TopologyBNLJMain {
    public static void main(String[] args) throws InterruptedException {

         /*Creation du spout Kafka pour Trident*/
        /*BrokerHosts zk = new ZkHosts("localhost:"+ MyConstants.KAFKA_ZK_PORT);

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME);
        spoutConf.fetchSizeBytes = 1000; //Sliding window*/

        int size = 380;
        int nbParts = 4;
        int k = 11;

        //OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        TridentTopology topology=new TridentTopology();

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("bytes"),20,
                new Values("44.3 3.3"),new Values("42.3 3.3"),new Values("44.0 3.3"),
                new Values("42.3 3.3"),new Values("40.3 3.3"),new Values("42.3 3.3"),
                new Values("43.1 3.3"),new Values("43.0 3.3"),new Values("42.3 3.3"),new Values("41.3 3.3"));

        spout.setCycle(true);

        List<String> outputFields = new ArrayList<>();
        outputFields.add("bytes");
        outputFields.add("input");

        List<Fields> joinFields = new ArrayList<>();

        Stream stream = topology.newStream("kafka-spout", spout)
                .shuffle()
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"))
                .parallelismHint(nbParts)
                .each(new Fields("input"), new InputNormalizerFunction(), new Fields("input2"))
                .shuffle();

        List<Stream> streams = new ArrayList<>();

        for(int i=0; i<nbParts; i++) {
            streams.add(stream.each(new Fields("input"), new InputCompareToDBFunction(k, HBaseDB.getIndiceDB(size, nbParts)[i], size / nbParts), new Fields("Partition S" + i)).parallelismHint(1));
            outputFields.add("Partition S" + i);
            joinFields.add(new Fields("bytes","input"));
        }

        topology.join(streams,joinFields,new Fields(outputFields))
        .each(new Fields(outputFields), new ReducekNNFunction(k, nbParts), new Fields("Finaloutput"));


        Config conf;
        conf = new Config();
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10);
        LocalCluster cluster = new LocalCluster();

        System.err.println("START!!!!!!!!!!!!!!!!!!!!");

        cluster.submitTopology("Trident-Topology", conf, topology.build());

    }
}
