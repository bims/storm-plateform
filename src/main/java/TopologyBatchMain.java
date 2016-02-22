import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import otherClass.MyConstants;
import tridentFunctions.*;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;

import java.util.ArrayList;
import java.util.Collections;


public class TopologyBatchMain {
    public static void main(String[] args) throws InterruptedException {

         /*Creation du spout Kafka pour Trident*/
        BrokerHosts zk = new ZkHosts("localhost:"+MyConstants.KAFKA_ZK_PORT);

      /*  Broker broker = new Broker("localhost:9092");
        GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
        GlobalPartitionInformation partitionInfo2 = new GlobalPartitionInformation();

        partitionInfo.addPartition(0, broker);
        //partitionInfo2.addPartition(1, broker);
        //partitionInfo.addPartition(2, broker);
        StaticHosts hosts = new StaticHosts(partitionInfo);
        //StaticHosts hosts2 = new StaticHosts(partitionInfo2);*/
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME);
        //spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        //spoutConf.fetchSizeBytes = 400; //Sliding window avec 11 messages

        int size = 380;
        int nbParts = 4;

        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        TridentTopology topology=new TridentTopology();

        topology.newStream("kafka-spout", spout)
                .parallelismHint(nbParts)
                .shuffle()
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"))
                .parallelismHint(nbParts)
                .each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size, nbParts)[0], size / nbParts), new Fields("Nimporte1"))
                .parallelismHint(nbParts)
                .each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size, nbParts)[1], size / nbParts), new Fields("Nimporte2"))
                .parallelismHint(nbParts)
                .each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size, nbParts)[2], size / nbParts), new Fields("Nimporte3"))
                .parallelismHint(nbParts)
                .each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size, nbParts)[3], size / nbParts), new Fields("Nimporte4"))
                .parallelismHint(nbParts)
                .each(new Fields("input", "Nimporte1", "Nimporte2", "Nimporte3", "Nimporte4"), new ReducekNNFunction(), new Fields("Finaloutput"))
                .parallelismHint(nbParts);
                //.each(new Fields("output"), new PrintFilter());
        //.aggregate(new Fields("output"), new ReducekNN(), new Fields("Finaloutput"));
               // .partitionAggregate(new Fields("bytes"), new TestAggregator(), new Fields("res"))
              //  .each(new Fields("res"), new PrintFilter());
        //.aggregate(new Fields("res"), new TestAggregator(), new Fields("res2"))
               // .groupBy()lobal()
           //     .each(new Fields("res2"), new PrintFilter());


        Config conf;
        conf = new Config();
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10);
        LocalCluster cluster = new LocalCluster();

        System.err.println("START!!!!!!!!!!!!!!!!!!!!");

        cluster.submitTopology("Trident-Topology", conf, topology.build());
    }

    //POUR RECUPERER LES BONS INDICES
    public static int[] getIndiceDB(int size, int nbParts){

        int[] res = new int[nbParts];

        //On récupère les indices réels que l'on souhaite
        for(int i = 0; i<nbParts; i++){

            res[i] = (i*size/nbParts)+1;

        }

        ArrayList<String> indices = new ArrayList<String>();

        //On cree une liste dans l'ordre numérique
        for(int i = 1; i<=size; i++){

            indices.add(""+i);

        }

        //On trie la liste par ordre alphabetique
        Collections.sort(indices);

        //On affecte le nouvel indice
        String newIndice = "";
        for(int i = 0; i<nbParts; i++){


            newIndice = indices.get((i*size/nbParts));

            res[i] = Integer.parseInt(newIndice);

            //System.out.println(newIndice);

        }

        return res;

    }
}