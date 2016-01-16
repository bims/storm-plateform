package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import inputClass.Input;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ResultScanner;
import otherClass.*;

import java.io.IOException;
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
		int k = 10;// # of neighbours


		//les données sont X et Y (INPUT)
		// TODO modifier
		double[] query = {Double.parseDouble(str.getX()), Double.parseDouble(str.getY())};


		//On récupère les restaurants
		Configuration config = HBaseConfiguration.create();
		HBaseDB listeRestaurants = new HBaseDB(config);

		double x;
		double y;
		String nomDuRestaurant;
		int limit = 380;

		config = HBaseConfiguration.create();
		config.addResource("hbase-site.xml");

		List<Restaurant> lr = null;

		try {

			//On récupère la liste des restaurants
			lr = listeRestaurants.ScanRows("1",limit);
			//un tableau pour contenir x et y (lon et lat)
			double[][] instancesResto = new double[limit+1][2];
			//list to save distance result
			List<Result> resultList = new ArrayList<Result>();

			for(int resto = 1; resto < limit; resto++) {

				//System.err.println("RESTO:"+resto+" x="+lr.get(resto).getLon()+" y="+lr.get(resto).getLat());

				x = Double.parseDouble(lr.get(resto).getLat());
				y = Double.parseDouble(lr.get(resto).getLon());

				String name = lr.get(resto).getName();
				//System.out.println("Name"+name);

				instancesResto[resto][0] = x;
				instancesResto[resto][1] = y;


				//find distances
				double dist = 0.0;
				for(int j = 0; j < 2; j++){
					dist += Math.pow(instancesResto[resto][j] - query[j], 2) ;
					//System.out.print("restaurant"+j+" "+restaurant.restaurantAttributes[j]+"\n");
				}

				double distance = Math.sqrt( dist );
				resultList.add(new Result(distance,name));
				//System.out.print("Restaurant:"+name+" X:"+instancesResto[resto][0]+" Y:"+instancesResto[resto][1]+"\n");
				//System.out.println("distance="+distance);



			}



			//System.out.println(resultList);
			Collections.sort(resultList, new DistanceComparator());
			//String[] ss = new String[k];

			System.err.println("\n\nX: " + str.getX() + " Y: " + str.getY());

			for(int v = 0; v < k; v++){
				System.err.println(resultList.get(v).restaurantName+ " .... " + resultList.get(v).distance);
				//get classes of k nearest instances (city names) from the list into an array
				//ss[x] = resultList.get(x).restaurantName;
			}

			System.err.println("\n\n");






		} catch (IOException e) {
			e.printStackTrace();
		}



	}








	/**
	 * Methode private static String findMajorityClass(String[] array) supprimee car inutilisee
	 */

	/**
	 * Methode private static double meanOfArray(double[] m) supprimee car inutilisee
	 */


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
