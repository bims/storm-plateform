import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import otherClass.MyConstants;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import tridentFunctions.InputNormalizerFunction;
import tridentFunctions.PartitionFilter;
import tridentFunctions.zValue.*;

import java.util.ArrayList;
import java.util.List;

public class TopologyZValueMain {
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

        Stream firstStream = topology.newStream("kafka-spout", spout)
                .shuffle()
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"))
                .each(new Fields("input"), new ZValueFunction(), new Fields("zValue"))
                .parallelismHint(nbParts) //Ce n'est pas necessaire
                .aggregate(new Fields("input", "zValue"), new SortAggregator(nbParts), new Fields("inputZValue", "numPartition"))
                .partitionBy(new Fields("numPartition"));

        //utiliser un filtre pour que chaque partition de R soit couplee a la bonne partition de S
        //On reprend une partie du code de BNLJ pour paralleliser le traitement de chaque partition
        List<Stream> streams = new ArrayList<>();
        for(int i=0; i<nbParts; i++){
            streams.add(firstStream.each(new Fields("inputZValue", "numPartition"), new PartitionFilter(i))
                    .each(new Fields("inputZValue", "numPartition"),
                            new ZkNNFunction(k, 0, size, nbParts, i), new Fields("res"))
                    .parallelismHint(nbParts));
        }

        //En commentaire ci-dessous, un code qui sert pour divers tests

      /* TridentTopology topology=new TridentTopology();
       FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"),20,
                new Values("blabla"),new Values("blabla2"),new Values("blabla1"),
                new Values("blabla3"),new Values("blabla4"),new Values("blabla5"),
               new Values("blabla6"),new Values("blabla7"),new Values("blabla8"));

       // spout.setCycle(true);

        Stream streamPartitions = topology.newStream("kafka-spout", spout)
                .shuffle();

        List<Stream> streams = new ArrayList<>();
        for(int i=0; i<2; i++){
            streams.add(streamPartitions.each(new Fields("sentence"), new PartitionFilter(i)).parallelismHint(3)
                    .each(new Fields("sentence"), new testFunction("Print"), new Fields("res")));
        }*/

      /*  Stream stream1 = stream
                .each(new Fields("sentence"), new testFunction("Flux1"), new Fields("output"))
                //.each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"));
                .each(new Fields("sentence"), new testFunction("Flux2"), new Fields("Hey"))
                .parallelismHint(2);*/
        //Stream finalStream = topology.merge(new Fields("sentence","output","Hey"), stream1,stream2);

        Config conf;
        conf = new Config();
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10);
        LocalCluster cluster = new LocalCluster();

        System.err.println("START!!!!!!!!!!!!!!!!!!!!");

        cluster.submitTopology("Trident-Topology", conf, topology.build());
    }
}
