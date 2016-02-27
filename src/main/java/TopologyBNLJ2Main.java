import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import otherClass.HBaseDB;
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
import tridentFunctions.PartitionFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sy306571 on 26/02/16.
 */
public class TopologyBNLJ2Main {
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

        Stream stream = topology.newStream("kafka-spout", spout)
                .shuffle()
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"))
                .parallelismHint(nbParts);

        //allStreams(i,j) contient RiSj
        List<List<Stream>> allStreams = new ArrayList<>();


        for(int i=0; i<nbParts; i++){
            List<Stream> streams = new ArrayList<>();
            Stream partitionStream = stream.each(new Fields("input"), new PartitionFilter(i));
            for(int j=0; j<nbParts; j++) {
                streams.add(partitionStream.each(new Fields("input"),
                        new InputCompareToDBFunction(k, HBaseDB.getIndiceDB(size, nbParts)[j], size / nbParts),
                        new Fields("Partition S" + j)));
            }
            allStreams.add(streams);
        }

        List<String> outputFields = new ArrayList<>();
        outputFields.add("bytes");
        outputFields.add("input");

        List<Fields> joinFields = new ArrayList<>();
        for(int i=0; i<nbParts; i++){
            outputFields.add("Partition S" + i);
            joinFields.add(new Fields("bytes","input"));
        }

        for(int i=0; i<nbParts; i++){
            //On applique la jointure sur les RiS(0-nbParts) puis on fait le Reduce kNN
            topology.join(allStreams.get(i),joinFields,new Fields(outputFields))
            .each(new Fields(outputFields), new ReducekNNFunction(k, nbParts), new Fields("Finaloutput"));
        }


        Config conf;
        conf = new Config();
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10);
        LocalCluster cluster = new LocalCluster();

        System.err.println("START!!!!!!!!!!!!!!!!!!!!");

        cluster.submitTopology("Trident-Topology", conf, topology.build());

    }
}
