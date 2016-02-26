package tridentFunctions;

import backtype.storm.tuple.Values;
import inputClass.Input;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import otherClass.HBaseDB;
import otherClass.Restaurant;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.*;

/**
 * Created by sy306571 on 16/01/16.
 */
public class InputCompareToDBFunction extends BaseFunction {

    private int k;
    private int borneInf;
    private int borneSup;
    private List<Restaurant> lr;
    private List<Restaurant> lr1;
    private List<Restaurant> lr2;
    private List<Restaurant> lr3;
    private List<Restaurant> lr4;


    //DEBUG
    Integer id;


    public InputCompareToDBFunction(int k, int borneInf, int borneSup){
        //liste des bornes inf en static: 1, 185, 270, 356
        this.k = k;
        this.borneInf = borneInf;
        this.borneSup = borneSup;


    }
    
    //@Override
    public void prepare(Map conf, TridentOperationContext context){
        Configuration config = HBaseConfiguration.create();
        config.addResource("hbase-site.xml");
        HBaseDB listeRestaurants = new HBaseDB(config);
        //On récupère la liste des restaurants de la base S
        try {
            lr1 = listeRestaurants.ScanRows(""+borneInf,borneSup); //On interroge HBase sur la partition 1
            //lr2 = listeRestaurants.ScanRows(""+185,borneSup); //On interroge HBase sur la partition 2
           // lr3 = listeRestaurants.ScanRows(""+270,borneSup); //On interroge HBase sur la partition 3
            //lr4 = listeRestaurants.ScanRows(""+356,borneSup); //On interroge HBase sur la partition 4
        } catch (IOException e) {
            e.printStackTrace();
        }
        //DEBUG
        this.id = context.getPartitionIndex();
    }
    
    public void execute(TridentTuple input, TridentCollector collector) {
        //DEBUG
        //On commence à l'id 1
        this.id = this.id+1;
        //System.err.println("BOLT_ID: "+this.id);

        //On recupère les données de l'input
        Input str = (Input) input.getValue(0);
        //On vérifie que l'on a bien recuperer les donnees de l'input
        //System.err.println("X: " + str.getX() + " Y: " + str.getY());
        
        //String numPart = (String) input.getValue(1);
        
       //if(numPart.contains("1")){
            
            lr = this.lr1;
            
        /*}else if(numPart.contains("2")){
            
            lr = this.lr2;
            
        }else if(numPart.contains("3")){
            
            lr = this.lr3;
            
        }else if(numPart.contains("4")){
            
            lr = this.lr4;
            
        }*/

        //on implemente KNN ici
        int k = this.k;// # of neighbours
        
        
        //les données sont X et Y (INPUT)
        // TODO modifier
        double[] query = {Double.parseDouble(str.getX()), Double.parseDouble(str.getY())};
        
        
        //On récupère les restaurants
        
        double x;
        double y;
        String nomDuRestaurant;
        int limit = borneSup;
        
        //un tableau pour contenir x et y (lon et lat)
        double[][] instancesResto = new double[limit+1][2];
        //list to save distance result
        List<Result> resultList = new ArrayList<Result>();
        
        for(int resto = 0; resto < limit; resto++) {
            
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
            resultList.add(new Result(distance,name,instancesResto[resto][0],instancesResto[resto][1]));
            //System.out.print("Restaurant:"+name+" X:"+instancesResto[resto][0]+" Y:"+instancesResto[resto][1]+"\n");
            //System.out.println("distance="+distance);
            
            
            
        }
        
        
        
        //System.out.println(resultList);
        Collections.sort(resultList, new DistanceComparator());

        //System.err.println("\n\nX: " + str.getX() + " Y: " + str.getY());
        
       /* for(int v = 0; v < k; v++){
            //System.err.println(resultList.get(v).restaurantName+ " .... " + resultList.get(v).distance);
            ss[v] = resultList.get(v).restaurantName;
            resultList.add(x)
            //get classes of k nearest instances (city names) from the list into an array
            //ss[x] = resultList.get(x).restaurantName;
        }*/
        resultList = resultList.subList(0,k);

        collector.emit(new Values(resultList));
        
    }


    //simple class to model results (distance + class)
    public static class Result {
        double distance;
        double x;
        double y;
        String restaurantName;
        public Result(double distance, String restaurantName, double x, double y){
            this.restaurantName = restaurantName;
            this.distance = distance;
            this.x =x;
            this.y=y;
        }
    }
    //simple comparator class used to compare results via distances
    public static class DistanceComparator implements Comparator<Result> {
        //@Override
        public int compare(Result a, Result b) {
            return a.distance < b.distance ? -1 : a.distance == b.distance ? 0 : 1;
        }
    }
    
    
}

