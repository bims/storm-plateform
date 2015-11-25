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
	Map<String, Integer> counters;

	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	@Override
	public void cleanup() {
	}

	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
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
		} catch (IOException e) {
			System.out.println(e);
		}

		//JSONParser parser = new JSONParser();

		//Object obj2 = null;
		//try {
		//	//ON PARSE LA STRING
		//	obj2 = parser.parse(stringDB);
		//} catch (ParseException e) {
		//	e.printStackTrace();
		//}

		//System.err.println(obj2);

		//On met les résultats dans un tableau
		//JSONArray arrayJSON;
		//arrayJSON = (JSONArray)obj2;

		//on affiche le premier élément
		//System.err.println(arrayJSON.get(1));


		System.err.println(stringDB);

	}


}
