package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import inputClass.Input;
import org.apache.storm.shade.org.json.simple.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class InputCompareToDB extends BaseBasicBolt {

	Integer id;
	String name;

	@Override
	public void cleanup() {
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	public void execute(Tuple input, BasicOutputCollector collector) {
		Input str = (Input) input.getValue(0);
		System.err.println("X: " + str.getX() + " Y: " + str.getY());

		//on cree un objet json pour récupérer le résultat.
		JSONObject obj = new JSONObject();

		//String pour recuperer la string du fichier
		String stringDB = "";

		//on lit le fichier
		Path path_DataBase = Paths.get("src/main/resources", "restaurant.json");
		try {
			byte[] ArrayDB = Files.readAllBytes(path_DataBase);

			stringDB = new String(ArrayDB, "ISO-8859-1");
			System.out.println(stringDB);
		}
		catch (IOException e) {
			System.out.println(e);
		}



		System.err.println(stringDB);

	}


}
