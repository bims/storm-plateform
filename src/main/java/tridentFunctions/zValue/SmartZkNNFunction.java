package tridentFunctions.zValue;


import convert_coord.Zorder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import otherClass.HBaseDB;
import otherClass.Restaurant;
import otherClass.ZLimits;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class SmartZkNNFunction extends BaseFunction {

    private HashMap<Integer,ZLimits> zLimits;
    private int k;
    private int size;
    private int nbParts;
    private int numPart;
    private List<Restaurant> lr;
    private List<Restaurant> lr1;
    private List<RestaurantZValue> restaurantZValues;


    public SmartZkNNFunction(int k, int size, int nbParts, int numPart, HashMap<Integer,ZLimits> zLimits){
        //liste des bornes inf en static: 1, 185, 270, 356
        this.k = k;
        this.size = size;
        this.nbParts = nbParts;
        this.numPart = numPart;
        this.zLimits = zLimits;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context){
        Configuration config = HBaseConfiguration.create();
        config.addResource("hbase-site.xml");
        HBaseDB listeRestaurants = new HBaseDB(config);

        //On récupère la liste des restaurants de la base S
        try {
            int borneInf = 0;
            int nbItems = 12;

            long nbTuplesByPartition = Math.round(((double) size)/((double) nbParts));
            int numPartition = 1;
            for(int i=0; i<size; i++) {
                if (i != 0 && numPartition < nbParts && i % nbTuplesByPartition == 0) {
                    numPartition++;
                }
            }

            lr1 = listeRestaurants.ScanRows(""+borneInf,nbItems);
            List<RestaurantZValue> restaurantZValues = new ArrayList<>();

            lr = this.lr1;
            int scale = 1000;

            for(Restaurant rest : lr) {

                double[] coord = new double[2];
                coord[0] = Double.parseDouble(rest.getLat());
                coord[1] = Double.parseDouble(rest.getLon());

                int[] convertCoord = Zorder.convertCoord(1, 2, scale, new int[2][2], coord);
                //int[] zValue = (coord[0] + coord[1]) * ((coord[0] + coord[1]));
                String zValue = String.valueOf(Zorder.eraseZeros(Zorder.valueOf(2, convertCoord)));

                restaurantZValues.add(new RestaurantZValue(rest.getId(), rest.getLat(), rest.getLon(), rest.getName(), new BigInteger(zValue)));
            }

            Collections.sort(restaurantZValues, new Comparator<RestaurantZValue>() {

                @Override
                public int compare(RestaurantZValue o1, RestaurantZValue o2) {
                    return o1.getzValue().compareTo(o2.getzValue());
                }
            });

            this.restaurantZValues = restaurantZValues.subList((restaurantZValues.size()/nbParts)*numPart,(restaurantZValues.size()/nbParts)*(numPart+1));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

    }
}
