import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import otherClass.MyConstants;
import storm.trident.Stream;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.FixedBatchSpout;
import tridentFunctions.*;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class TopologyBatchMain {
    public static void main(String[] args) throws InterruptedException {

         /*Creation du spout Kafka pour Trident*/
        BrokerHosts zk = new ZkHosts("localhost:"+MyConstants.KAFKA_ZK_PORT);

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME);
        spoutConf.fetchSizeBytes = 400; //Sliding window avec 11 messages

        int size = 380;
        int nbParts = 4;

        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        TridentTopology topology=new TridentTopology();


        Stream stream = topology.newStream("kafka-spout", spout)
                .shuffle()
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"))
                .parallelismHint(nbParts);

        List<String> outputFields = new ArrayList<>();
        outputFields.add("input");

        for(int i=0; i<nbParts; i++){
            stream = stream.each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size, nbParts)[i], size / nbParts), new Fields("Partition S" + i));
            outputFields.add("Partition S" + i);
        }

              /*  .each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size, nbParts)[1], size / nbParts), new Fields("Nimporte2"))
                .each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size, nbParts)[2], size / nbParts), new Fields("Nimporte3"))
                .each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size, nbParts)[3], size / nbParts), new Fields("Nimporte4"))*/
        stream.each(new Fields(outputFields), new ReducekNNFunction(nbParts), new Fields("Finaloutput"));



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