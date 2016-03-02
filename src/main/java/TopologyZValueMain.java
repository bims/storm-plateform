import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import otherClass.HBaseDB;
import otherClass.MyConstants;
import otherClass.ZLimits;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import tridentFunctions.InputCompareToDBFunction;
import tridentFunctions.InputNormalizerFunction;
import tridentFunctions.PartitionFilter;
import tridentFunctions.ReducekNNFunction;
import tridentFunctions.zValue.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
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

        int nbTuples[] = new int[nbParts];
        int remaining = size;
        int h=0;
        while(remaining > 0){
            nbTuples[h]++;
            remaining--;
            h++;
            if(h==nbParts)
                h=0;
        }

        int startId[] = new int[nbParts];
        int sum = 0;
        for(h=0; h<nbParts; h++){
            startId[h] = sum;
            sum+=nbTuples[h];
        }



        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        TridentTopology topology=new TridentTopology();

        Stream firstStream = topology.newStream("kafka-spout", spout)
                .shuffle()
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"))
                .each(new Fields("input"), new ZValueFunction(), new Fields("zValue"))
                //.parallelismHint(nbParts) //Ce n'est pas necessaire
                .aggregate(new Fields("input", "zValue"), new SortAggregator(nbParts), new Fields("inputZValue", "numPartition"))
                .partitionBy(new Fields("numPartition"));

        //allStreams(i,j) contient RiSj
        List<List<Stream>> allStreams = new ArrayList<>();

        for(int i=0; i<nbParts; i++){
            List<Stream> streams = new ArrayList<>();
            Stream partitionStream = firstStream.each(new Fields("inputZValue"), new PartitionFilter(i))
                    .parallelismHint(nbParts)
                    .shuffle();
            for(int j=0; j<nbParts; j++) {
                streams.add(partitionStream.each(new Fields("inputZValue"),
                        new ZkNNFunction(k, startId[j], nbTuples[j], nbParts, j),
                        new Fields("Partition S" + j))
                        .parallelismHint(1));
            }
            allStreams.add(streams);
        }

        List<String> outputFields = new ArrayList<>();
        outputFields.add("inputZValue");
        outputFields.add("numPartition");


        List<Fields> joinFields = new ArrayList<>();

        for(int i=0; i<nbParts; i++){
            outputFields.add("Partition S" + i);
            joinFields.add(new Fields("inputZValue","numPartition"));
        }

        for(int i=0; i<nbParts; i++){
            //On applique la jointure sur les RiS(0-nbParts) puis on fait le Reduce kNN
            topology.join(allStreams.get(i),joinFields,new Fields(outputFields))
                    .each(new Fields(outputFields), new ReducezkNNFunction(k,nbParts), new Fields("Finaloutput"));
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
