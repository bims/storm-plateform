import spouts.InputReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCounter;
import bolts.InputNormalizer;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();

		//P1: SPOUT
		builder.setSpout("input-reader",new InputReader());

		//P2: BOLTS
		//BOLT 1
		builder.setBolt("input-normalizer", new InputNormalizer()).shuffleGrouping("input-reader");
		//BOLT 2
		builder.setBolt("word-counter", new WordCounter(),1).fieldsGrouping("input-normalizer", new Fields("word"));


        //Configuration
		Config conf;
		conf = new Config();
		conf.put("inputFile", args[0]);
		conf.setDebug(true);

        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();

		System.err.println("START!!!!!!!!!!!!!!!!!!!!");

		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());

		System.err.println("Thread.sleep(2000)!!!!!!!!!!!!!!!!!!!!");

		Thread.sleep(2000);
		cluster.shutdown();

		System.err.println("END!!!!!!!!!!!!!!!!!!!!");
	}
}
