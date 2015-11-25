import spouts.InputReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.InputCompareToDB;
import bolts.InputNormalizer;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("input-reader",new InputReader());

		builder.setBolt("input-normalizer", new InputNormalizer()).shuffleGrouping("input-reader");
		builder.setBolt("input-compareToDB", new InputCompareToDB(),1).fieldsGrouping("input-normalizer", new Fields("inputcoord"));

		Config conf;
		conf = new Config();
		conf.put("inputFile", args[0]);
		conf.setDebug(false);

		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();

		System.err.println("START!!!!!!!!!!!!!!!!!!!!");

		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());

		System.err.println("Thread.sleep(2000)!!!!!!!!!!!!!!!!!!!!");

		Thread.sleep(6000);
		cluster.shutdown();

		System.err.println("END!!!!!!!!!!!!!!!!!!!!");
	}
}
