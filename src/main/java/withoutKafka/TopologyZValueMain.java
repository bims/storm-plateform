package withoutKafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import hbase.HBaseDB;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import tridentFunctions.PartitionFilter;
import tridentFunctions.zValue.ReducezkNNFunction;
import tridentFunctions.zValue.SortAggregator;
import tridentFunctions.zValue.ZValueFunction;
import tridentFunctions.zValue.ZkNNFunction;

import java.util.ArrayList;
import java.util.List;

public class TopologyZValueMain {
    public static void main(String[] args) throws InterruptedException {

        int size = 380;
        int nbParts = 2;
        int k = 11;

        int nbTuples[] = HBaseDB.getNbItems(size,nbParts);
        int startId[] = HBaseDB.getStartIds(nbParts,nbTuples);

        TridentTopology topology=new TridentTopology();

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("bytes"),20,
                new Values("44.3 3.3"),new Values("42.3 3.3"),new Values("44.0 3.3"),
                new Values("42.3 3.3"),new Values("40.3 3.3"),new Values("42.3 3.3"),
                new Values("43.1 3.3"),new Values("43.0 3.3"),new Values("42.3 3.3"),new Values("41.3 3.3"));

        spout.setCycle(true);

        Stream firstStream = topology.newStream("kafka-spout", spout)
                .shuffle()
                .each(new Fields("bytes"), new withoutKafka.InputNormalizerFunction(), new Fields("input"))
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
