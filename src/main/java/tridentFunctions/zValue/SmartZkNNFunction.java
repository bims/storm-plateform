package tridentFunctions.zValue;


import backtype.storm.tuple.Values;
import convert_coord.Zorder;
import inputClass.InputZValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import otherClass.HBaseDB;
import otherClass.Restaurant;
import otherClass.ZLimits;
import otherClass.parseJSONtoDB;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class SmartZkNNFunction extends BaseFunction {

    private int k;
    private int borneInf;
    private int nbItems;
    private List<RestaurantZValue> restaurantZValues;
    private int numPart;

    public SmartZkNNFunction(int k, int borneInf, int nbItems, int numPart){
        //liste des bornes inf en static: 1, 185, 270, 356
        this.k = k;
        this.borneInf = borneInf;
        this.nbItems = nbItems;
        this.numPart = numPart;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context){
        Configuration config = HBaseConfiguration.create();
        config.addResource("hbase-site.xml");
        HBaseDB listeRestaurants = new HBaseDB(config);

        //On récupère la liste des restaurants de la base S
        try {

            restaurantZValues = listeRestaurants.ScanZRows(parseJSONtoDB.zeroPadding(""+borneInf),nbItems);

           // zLimits.put(numPart,new ZLimits(restaurantZValues.get(0).getzValue(),
           //         restaurantZValues.get(restaurantZValues.size()-1).getzValue()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(TridentTuple input, TridentCollector collector) {
        InputZValue str = (InputZValue) input.getValue(0);

        //on implemente KNN ici
        int k = this.k;// # of neighbours


        RestaurantZValue rZ = new RestaurantZValue("",str.getX(),str.getY(),"requete",str.getzValue());

        //Calcule les k top voisins
        List<RestaurantZValue> candidats;
        if(restaurantZValues.size() > k){
            TreeSet<RestaurantZValue> treeSet = new TreeSet<>(restaurantZValues);
            candidats = ZkNNFunction.treeSetInsert(treeSet, rZ, k);
        }
        else candidats = restaurantZValues;

        List<Result> resultList = new ArrayList<Result>();
        for(int i=0; i<candidats.size(); i++){
            RestaurantZValue restZ = candidats.get(i);
            //Calcul de la distance
            BigInteger distance = rZ.getzValue().subtract(restZ.getzValue());
            resultList.add(new Result(distance.abs(), restZ.getName(),
                    Double.parseDouble(restZ.getLat()),
                    Double.parseDouble(restZ.getLon())));
        }

        Collections.sort(resultList, new Comparator<Result>() {
            @Override
            public int compare(Result o1, Result o2) {
                return o1.distance.compareTo(o2.distance);
            }
        });

        //collector.emit(new Values(resultList));

        String resString = "\n\nPartition "+numPart+"\nX:" + str.getX() + " Y:" + str.getY()+"\n";

        for (int v = 0; v < k; v++) {
            resString += resultList.get(v).restaurantName + " ....*******kNN Global " + resultList.get(v).distance + " x: " + resultList.get(v).x + " y:" + resultList.get(v).y+"\n";
            //get classes of k nearest instances (city names) from the list into an array
            //ss[x] = resultList.get(x).restaurantName;
        }
        resString += "\n\n";
        System.err.println(resString);
    }
}
