package tridentFunctions.zValue;

import backtype.storm.tuple.Values;
import inputClass.InputZValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import hbase.HBaseDB;
import hbase.parseJSONtoDB;
import otherClass.RestaurantZValue;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by sy306571 on 24/02/16.
 */
public class ZkNNFunction extends BaseFunction {


    private int k;
    private int borneInf;
    private int nbItems;
    private int nbParts;
    private int numPart;
    private List<RestaurantZValue> lr;
    private List<RestaurantZValue> restaurantZValues;


    public ZkNNFunction(int k, int borneInf, int nbItems, int nbParts, int numPart){
        //liste des bornes inf en static: 1, 185, 270, 356
        this.k = k;
        this.borneInf = borneInf;
        this.nbItems = nbItems;
        this.nbParts = nbParts;
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

            //this.restaurantZValues = lr.subList((nbItems/nbParts)*numPart,(nbItems/nbParts)*(numPart+1));



        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(TridentTuple input, TridentCollector collector) {
        InputZValue str = (InputZValue) input.getValue(0);

        //on implemente KNN ici
        int k = this.k;// # of neighbours


        RestaurantZValue rZ = new RestaurantZValue("",str.getX(),str.getY(),"requete",str.getzValue());

        //Calcule les k top voisins
        List<RestaurantZValue> candidats;
        if(restaurantZValues.size() > k){
            TreeSet<RestaurantZValue> treeSet = new TreeSet<>(restaurantZValues);
            candidats = treeSetInsert(treeSet,rZ,k);
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

        collector.emit(new Values(resultList));

    }

    public static List<RestaurantZValue> treeSetInsert(TreeSet<RestaurantZValue> treeSet, RestaurantZValue rZ, int k){
        treeSet.add(rZ);
        TreeSet<RestaurantZValue> zMoins = (TreeSet<RestaurantZValue>) treeSet.headSet(rZ);
        SortedSet<RestaurantZValue> zPlus = treeSet.tailSet(rZ,false);

        List<RestaurantZValue> resultList = new ArrayList<>();

        Iterator<RestaurantZValue> it = zMoins.descendingIterator();
        int i=0;
        while(i<zMoins.size() && i<k){
            resultList.add(it.next());
            i++;
        }

        it = zPlus.iterator();
        i=0;
        while(i<zPlus.size() && i<k){
            resultList.add(it.next());
            i++;
        }

        return resultList;
    }


}
