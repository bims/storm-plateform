package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import inputClass.Input;

import java.util.*;


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

		//On recupère les données de l'input
		Input str = (Input) input.getValue(0);
		//On vérifie que l'on a bien recuperer les donnees de l'input
		//System.err.println("X: " + str.getX() + " Y: " + str.getY());


		//on implemente KNN ici
		int k = 3;// # of neighbours


		//les données sont X et Y et no du restaurant
		// TODO modifier
		double[] query = {Double.parseDouble(str.getX()), Double.parseDouble(str.getY())};


		//Liste des restaurants (DEBUG en attendant la lecture de la base de données)
		// TODO modifier
		double[][] instances = {
				{43.6958077, 7.2420017},
				{43.7224091, 7.1153905},
				{43.6279629, 7.0975711},
				{43.6270306, 7.0991356},
				{43.7084020, 7.2611232},
				{43.7080155, 7.2600723},
				{43.7069961, 7.2571355},
				{43.6960187, 7.2708430},
				{43.6960313, 7.2710951},
				{43.6956094, 7.2733957},
				{43.6915882, 7.2913772}
		} ;



		//Constructeur des restaurants
		// TODO modifier aussi
		List<Restaurant> restaurantList = new ArrayList<Restaurant>();
		//list to save distance result
		List<Result> resultList = new ArrayList<Result>();
		// add restaurant data to restaurantList
		restaurantList.add(new Restaurant(instances[0],"Restaurant1"));
		restaurantList.add(new Restaurant(instances[1],"Restaurant2"));
		restaurantList.add(new Restaurant(instances[2],"Restaurant3"));
		restaurantList.add(new Restaurant(instances[3],"Restaurant4"));
		restaurantList.add(new Restaurant(instances[4],"Restaurant5"));
		restaurantList.add(new Restaurant(instances[5],"Restaurant6"));
		restaurantList.add(new Restaurant(instances[6],"Restaurant7"));
		restaurantList.add(new Restaurant(instances[7],"Restaurant8"));
		restaurantList.add(new Restaurant(instances[8],"Restaurant9"));

		//find distances
		int nbR = 1;
		for(Restaurant restaurant : restaurantList){

			double dist = 0.0;
			for(int j = 0; j < restaurant.restaurantAttributes.length; j++){
				dist += Math.pow(restaurant.restaurantAttributes[j] - query[j], 2) ;
				//System.out.print("restaurant"+j+" "+restaurant.restaurantAttributes[j]+"\n");
			}
			double distance = Math.sqrt( dist );
			resultList.add(new Result(distance,restaurant.restaurantName));
			System.out.print("restaurant"+nbR+" X:"+restaurant.restaurantAttributes[0]+" Y:"+restaurant.restaurantAttributes[1]+"\n");
			System.out.println("distance="+distance);
			nbR++;
		}

		//System.out.println(resultList);
		Collections.sort(resultList, new DistanceComparator());
		//String[] ss = new String[k];

		System.err.println("X: " + str.getX() + " Y: " + str.getY());

		for(int x = 0; x < k; x++){
			System.out.println(resultList.get(x).restaurantName+ " .... " + resultList.get(x).distance + " X=" + restaurantList.get(x).restaurantAttributes[0] +  " et Y=" + restaurantList.get(x).restaurantAttributes[1]);
			//get classes of k nearest instances (city names) from the list into an array
			//ss[x] = resultList.get(x).restaurantName;
		}

	}








	/**
	 * Methode private static String findMajorityClass(String[] array) supprimee car inutilisee
	 */

	/**
	 * Methode private static double meanOfArray(double[] m) supprimee car inutilisee
	 */

	//simple class to model instances (features + class)
	static class Restaurant {
		double[] restaurantAttributes;
		String restaurantName;
		public Restaurant(double[] restaurantAttributes, String restaurantName){
			this.restaurantName = restaurantName;
			this.restaurantAttributes = restaurantAttributes;
		}
	}
	//simple class to model results (distance + class)
	static class Result {
		double distance;
		String restaurantName;
		public Result(double distance, String restaurantName){
			this.restaurantName = restaurantName;
			this.distance = distance;
		}
	}
	//simple comparator class used to compare results via distances
	static class DistanceComparator implements Comparator<Result> {
		//@Override
		public int compare(Result a, Result b) {
			return a.distance < b.distance ? -1 : a.distance == b.distance ? 0 : 1;
		}
	}


}
