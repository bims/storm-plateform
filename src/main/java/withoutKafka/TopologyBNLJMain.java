package withoutKafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import hbase.HBaseDB;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import tridentFunctions.InputCompareToDBFunction;
import tridentFunctions.ReducekNNFunction;

import java.util.ArrayList;
import java.util.List;


public class TopologyBNLJMain {
    public static void main(String[] args) throws InterruptedException {

         /*Creation du spout Kafka pour Trident*/
        //BrokerHosts zk = new ZkHosts("localhost:"+MyConstants.KAFKA_ZK_PORT);

        /*TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME);
        spoutConf.fetchSizeBytes = 1000; //Sliding window*/

        int size = 380;
        int nbParts = 4;
        int k = 11;

        //OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("bytes"),20,
                new Values("44.3 3.3"),new Values("42.3 3.3"),new Values("44.0 3.3"),
                new Values("42.3 3.3"),new Values("40.3 3.3"),new Values("42.3 3.3"),
        new Values("43.1 3.3"),new Values("43.0 3.3"),new Values("42.3 3.3"),new Values("41.3 3.3"));

        spout.setCycle(true);

        TridentTopology topology=new TridentTopology();

        Stream stream = topology.newStream("kafka-spout", spout)
                .shuffle()
                .each(new Fields("bytes"), new withoutKafka.InputNormalizerFunction(), new Fields("input"))
                .parallelismHint(nbParts);

        List<String> outputFields = new ArrayList<>();
        outputFields.add("input");

        for(int i=0; i<nbParts; i++){
            stream = stream.each(new Fields("input"), new InputCompareToDBFunction(k, HBaseDB.getIndiceDB(size, nbParts)[i], size / nbParts), new Fields("Partition S" + i));
            outputFields.add("Partition S" + i);
        }

        stream.each(new Fields(outputFields), new ReducekNNFunction(k, nbParts), new Fields("Finaloutput"));



        Config conf;
        conf = new Config();
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10);
        LocalCluster cluster = new LocalCluster();

        System.err.println("START!!!!!!!!!!!!!!!!!!!!");

        cluster.submitTopology("Trident-Topology", conf, topology.build());
    }

}