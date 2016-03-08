import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import hbase.HBaseDB;
import otherClass.MyConstants;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import tridentFunctions.InputCompareToDBFunction;
import tridentFunctions.InputNormalizerFunction;
import tridentFunctions.ReducekNNFunction;

import java.util.ArrayList;
import java.util.List;

public class TopologyBNLJ2Main {
    public static void main(String[] args) throws InterruptedException {

         /*Creation du spout Kafka pour Trident*/
        BrokerHosts zk = new ZkHosts("localhost:"+ MyConstants.KAFKA_ZK_PORT);

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME);
        spoutConf.fetchSizeBytes = 1000; //Sliding window

        int size = 382;
        int nbParts = 4;
        int k = 11;
        if(args.length == 2){
            nbParts = Integer.parseInt(args[0]);
            k = Integer.parseInt(args[1]);
        }

        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        TridentTopology topology=new TridentTopology();


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
