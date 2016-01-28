import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import otherClass.MyConstants;
import tridentFunctions.InputCompareToDBFunction;
import tridentFunctions.InputNormalizerFunction;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import tridentFunctions.ReducekNN;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by sy306571 on 16/01/16.
 */
public class TopologyBatchMain {
    public static void main(String[] args) throws InterruptedException {

         /*Creation du spout Kafka pour Trident*/
        BrokerHosts zk = new ZkHosts("localhost:"+MyConstants.KAFKA_ZK_PORT);

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, MyConstants.TOPIC_NAME);

        int size = 380;
        int nbParts = 4;

        //spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        //L'initialisation d'autres parametres est necessaire pour faire du batching

        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        TridentTopology topology=new TridentTopology();

        topology.newStream("kafka-spout", spout)
                .each(new Fields("bytes"), new InputNormalizerFunction(), new Fields("input"))
                .each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size,nbParts)[0],size/nbParts), new Fields("Nimporte1"))
                .each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size,nbParts)[1],size/nbParts), new Fields("Nimporte2"))
                .each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size,nbParts)[2],size/nbParts), new Fields("Nimporte3"))
                .each(new Fields("input"), new InputCompareToDBFunction(getIndiceDB(size,nbParts)[3],size/nbParts), new Fields("Nimporte4"))
                .aggregate(new Fields("input","Nimporte1", "Nimporte2", "Nimporte3", "Nimporte4"), new ReducekNN(), new Fields("Finaloutput"));


        Config conf;
        conf = new Config();
        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
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