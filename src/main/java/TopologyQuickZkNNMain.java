import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import otherClass.HBaseDB;
import otherClass.MyConstants;
import otherClass.ZLimits;
import otherClass.parseJSONtoDB;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import testTridentFunctions.testFunction;
import tridentFunctions.InputNormalizerFunction;
import tridentFunctions.PartitionFilter;
import tridentFunctions.zValue.*;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TopologyQuickZkNNMain {
    public static void main(String[] args) throws InterruptedException, IOException {
        /*Creation du spout Kafka pour Trident*/
        BrokerHosts zk = new ZkHosts("localhost:"+ MyConstants.KAFKA_ZK_PORT);

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME);
        //spoutConf.fetchSizeBytes = 1000; //Sliding window

        int size = 380;
        int nbParts = 2;
        int k = 11;

        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        TridentTopology topology=new TridentTopology();

        int nbTuples[] = HBaseDB.getNbItems(size, nbParts);
        int startId[] = HBaseDB.getStartIds(nbParts,nbTuples);

        Configuration config = HBaseConfiguration.create();
        config.addResource("hbase-site.xml");
        HBaseDB restaurantsDB = new HBaseDB(config);
        List<RestaurantZValue> restaurantZValues;
        HashMap<Integer, ZLimits> zLimits = new HashMap<>();

        for(int i=0; i<nbParts; i++){
            restaurantZValues = restaurantsDB.ScanZRows(parseJSONtoDB.zeroPadding("" + startId[i]),nbTuples[i]);
            //System.out.println("**********"+restaurantZValues.get(0).getName()+"****************");
            zLimits.put(i,new ZLimits(restaurantZValues.get(0).getzValue(),
                    restaurantZValues.get(restaurantZValues.size()-1).getzValue()));
        }

        /*topology.newStream("kafka-spout", spout)
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"))
                .each(new Fields("input"), new ZValueFunction(), new Fields("zValue"))
                .aggregate(new Fields("input", "zValue"), new SortAggregator(nbParts), new Fields("inputZValue", "numPartition"))
                .partitionBy(new Fields("numPartition"))
                .each(new Fields("inputZValue"), new testFunction("f"), new Fields("blabla"))
                .parallelismHint(nbParts)
                .shuffle()
                .each(new Fields("blabla"), new testFunction("Hey! "), new Fields("blabla2"))
                .parallelismHint(1);*/

       Stream firstStream = topology.newStream("kafka-spout", spout)
                .shuffle()
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"))
                .each(new Fields("input"), new ZValueFunction(), new Fields("zValue"))
                        //.parallelismHint(nbParts) //Ce n'est pas necessaire
                .each(new Fields("input", "zValue"), new SmartPartitionsFunction(zLimits, nbParts), new Fields("inputZValue", "numPartition"))
               .partitionBy(new Fields("numPartition"));

        List<Stream> streams = new ArrayList<>();
        for(int i=0; i<nbParts; i++){
            streams.add(firstStream.each(new Fields("inputZValue", "numPartition"), new SmartPartitionFilter(i))
                    .parallelismHint(nbParts) //Pas vraiment necessaire
                    .shuffle()
                    .each(new Fields("inputZValue", "numPartition"),
                            new SmartZkNNFunction(k, startId[i], nbTuples[i], i), new Fields("res"))
                    .parallelismHint(1));
        }

        Config conf;
        conf = new Config();
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();

        System.err.println("START!!!!!!!!!!!!!!!!!!!!");

        cluster.submitTopology("Trident-Topology", conf, topology.build());

    }
}