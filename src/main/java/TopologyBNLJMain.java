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
import testTridentFunctions.testFunction;
import testTridentFunctions.testFunction2;
import tridentFunctions.InputCompareToDBFunction;
import tridentFunctions.InputNormalizerFunction;
import tridentFunctions.ReducekNNFunction;

import java.util.ArrayList;
import java.util.List;

public class TopologyBNLJMain {
    public static void main(String[] args) throws InterruptedException {

         /*Creation du spout Kafka pour Trident*/
        BrokerHosts zk = new ZkHosts("localhost:"+ MyConstants.KAFKA_ZK_PORT);

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME);
        spoutConf.fetchSizeBytes = 1000; //Sliding window

        int size = 380;
        int nbParts = 4;
        int k = 11;

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

        Stream finalStream = topology.join(streams,joinFields,new Fields(outputFields));
        finalStream.each(new Fields(outputFields), new ReducekNNFunction(k, nbParts), new Fields("Finaloutput"));


        //En commentaire ci-dessous, un code qui sert pour divers tests
       /*
       TridentTopology topology=new TridentTopology();
       FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"),10,
                new Values("blabla"),new Values("blabla2"),new Values("blabla1"),
                new Values("blabla3"),new Values("blabla4"),new Values("blabla5"));

       // spout.setCycle(true);

        Stream stream = topology.newStream("kafka-spout", spout);
                //.shuffle().parallelismHint(2);
        Stream stream1 = stream
                .each(new Fields("sentence"), new testFunction("Flux1"), new Fields("output"));
                //.each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"));

        Stream stream2 = stream.each(new Fields("sentence"), new testFunction("Flux2"), new Fields("Hey"));
        //Stream finalStream = topology.merge(new Fields("sentence","output","Hey"), stream1,stream2);
        List<Stream> streams = new ArrayList<>();
        streams.add(stream1);
        streams.add(stream2);
        List<Fields> fieldses = new ArrayList<>();
        fieldses.add(new Fields("sentence"));
        fieldses.add(new Fields("sentence"));


        Stream finalStream = topology.join(streams,fieldses,new Fields("sentence","j","g"));
        finalStream.each(new Fields("j","g"), new testFunction2("Flux3"), new Fields("blabla"));*/




        Config conf;
        conf = new Config();
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10);
        LocalCluster cluster = new LocalCluster();

        System.err.println("START!!!!!!!!!!!!!!!!!!!!");

        cluster.submitTopology("Trident-Topology", conf, topology.build());

    }
}
