package tridentFunctions.zValue;

import convert_coord.Zorder;
import inputClass.Input;
import inputClass.InputZValue;
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

/**
 * Created by sy306571 on 24/02/16.
 */
public class ZkNNFunction extends BaseFunction {


    private int k;
    private int borneInf;
    private int borneSup;
    private int nbParts;
    private int numPart;
    private List<Restaurant> lr;
    private List<Restaurant> lr1;
    private IntelligentPartitionsFunction fct;
    private List<RestaurantZValue> restaurantZValues;


    public ZkNNFunction(int k, int borneInf, int borneSup, int nbParts, int numPart, IntelligentPartitionsFunction fct){
        //liste des bornes inf en static: 1, 185, 270, 356
        this.k = k;
        this.borneInf = borneInf;
        this.borneSup = borneSup;
        this.nbParts = nbParts;
        this.numPart = numPart;
        this.fct = fct;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context){
        Configuration config = HBaseConfiguration.create();
        config.addResource("hbase-site.xml");
        HBaseDB listeRestaurants = new HBaseDB(config);
        //On récupère la liste des restaurants de la base S
        try {
            lr1 = listeRestaurants.ScanRows(""+borneInf,borneSup);
            List<RestaurantZValue> restaurantZValues = new ArrayList<>();

            lr = this.lr1;
            int scale = 1000;

            for(Restaurant rest : lr) {

                double[] coord = new double[2];
                coord[0] = Double.parseDouble(rest.getLat());
                coord[1] = Double.parseDouble(rest.getLon());

                int[] convertCoord = Zorder.convertCoord(1, 2, scale, new int[2][2], coord);
                //int[] zValue = (coord[0] + coord[1]) * ((coord[0] + coord[1]));
                String zValue = String.valueOf(Zorder.fromStringToInt(Zorder.valueOf(2, convertCoord)));

                restaurantZValues.add(new RestaurantZValue(rest.getId(), rest.getLat(), rest.getLon(), rest.getName(), new BigInteger(zValue)));
            }

            Collections.sort(restaurantZValues, new Comparator<RestaurantZValue>() {

                @Override
                public int compare(RestaurantZValue o1, RestaurantZValue o2) {
                    return o1.getzValue().compareTo(o2.getzValue());
                }
            });

            this.restaurantZValues = restaurantZValues.subList((restaurantZValues.size()/nbParts)*numPart,(restaurantZValues.size()/nbParts)*(numPart+1));
            fct.getzLimits().put(numPart, new ZLimits(restaurantZValues.get(0).getzValue(),
                    restaurantZValues.get(restaurantZValues.size() - 1).getzValue()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(TridentTuple input, TridentCollector collector) {
        InputZValue str = (InputZValue) input.getValue(0);

        //on implemente KNN ici
        int k = this.k;// # of neighbours


        RestaurantZValue rZ = new RestaurantZValue("",str.getX(),str.getY(),"requete",str.getzValue());

        //Fonction permettant de calculer les k top voisins
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

        resultList = resultList.subList(0,k);
        String resString = "\n\nX: " + str.getX() + " Y: " + str.getY()+"\n";

        for (int v = 0; v < k; v++) {
            resString += resultList.get(v).restaurantName + " ....*******kNN Global " + resultList.get(v).distance + " x: "
                    + resultList.get(v).x + " y:" + resultList.get(v).y+"\n";
        }
        resString += "\n\n";
        System.err.println(resString);

    }


    //simple class to model results (distance + class)
    public static class Result {
        BigInteger distance;
        double x;
        double y;
        String restaurantName;
        public Result(BigInteger distance, String restaurantName, double x, double y){
            this.restaurantName = restaurantName;
            this.distance = distance;
            this.x =x;
            this.y=y;
        }
    }

    public static List<RestaurantZValue> treeSetInsert(TreeSet<RestaurantZValue> treeSet, RestaurantZValue rZ, int k){
        treeSet.add(rZ);
        TreeSet<RestaurantZValue> zMoins = (TreeSet<RestaurantZValue>) treeSet.headSet(rZ);
        SortedSet<RestaurantZValue> zPlus = treeSet.tailSet(rZ,false);

        List<RestaurantZValue> resultList = new ArrayList<>();
        int nbZMoins;
        int nbZPlus;
        if(zMoins.size() < k && zPlus.size() < k){
            nbZMoins = zMoins.size();
            nbZPlus = zPlus.size();
        }
        else if (zMoins.size() > k && zPlus.size() < k) {
            nbZMoins = k;
            nbZPlus = zPlus.size();
        } else if (zPlus.size() > k && zMoins.size() < k) {
            nbZMoins = zMoins.size();
            nbZPlus = k;
        } else {
            nbZMoins = nbZPlus = k;
        }

        Iterator<RestaurantZValue> it = zMoins.descendingIterator();
        for (int i = 0; i < nbZMoins; i++) {
            resultList.add(it.next());
        }

        it = zPlus.iterator();
        for(int i=0; i<nbZPlus; i++){
                resultList.add(it.next());
        }

        return resultList;
    }

    private static class RestaurantZValue implements Comparable<RestaurantZValue>{
        private String id;
        private String lat;
        private String lon;
        private String name;
        private BigInteger zValue;

        public RestaurantZValue(String id, String lat, String lon, String name, BigInteger zValue){
            this.id = id;
            this.lat = lat;
            this.lon = lon;
            this.zValue = zValue;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getLat() {
            return lat;
        }

        public String getLon() {
            return lon;
        }

        public BigInteger getzValue() {
            return zValue;
        }

        public String getName() {
            return name;
        }

        @Override
        public int compareTo(RestaurantZValue o) {
            int res = this.zValue.compareTo(o.getzValue());
            if(res==0){
                return this.getName().compareTo(o.getName());
            }
            else return res;
        }

        @Override
        public String toString() {
            return "x:"+lat+"-y:"+lon+"-v:"+zValue;
        }
    }

    public static void main(String[] args){

        /*List<RestaurantZValue> maListe = new ArrayList<>();
        for(int i=0; i<30; i++){
            Random rd = new Random();
            int lat = rd.nextInt(40);
            int lon = rd.nextInt(40);
            double numRest = Math.random();
            int entier = (lat+lon)*(lat+lon);
            maListe.add(new RestaurantZValue("",""+lat,""+lon,"Rest"+numRest,new BigInteger(""+entier)));
        }
        //System.out.println(maListe);
        Collections.sort(maListe);
        //System.out.println(maListe);
        TreeSet<RestaurantZValue> treeSet = new TreeSet<RestaurantZValue>(maListe);
        //System.out.println(treeSet);
        int[] query = {Integer.parseInt("7"), Integer.parseInt("12")};

        //On utilise un objet RestaurantZValue pour y mettre la query et sa zvalue
        // zValue = Zorder.convertCoord(1,2,1000,new int[2][2],query);
        int zValue = (query[0]+query[1])*(query[0]+query[1]);
        System.out.println("Requete : "+zValue);
        RestaurantZValue rZ = new RestaurantZValue("","23.0","24.0","requete",new BigInteger(""+zValue));

        int k = 4;
        //Fonction permettant de calculer les k top voisins
        List<RestaurantZValue> res = treeSetInsert(treeSet,rZ,k);
        System.out.println(res);

        List<Result> resultList = new ArrayList<Result>();
        for(int i=0; i<res.size(); i++){
            RestaurantZValue restZ = res.get(i);
            resultList.add(new Result(rZ.getzValue().subtract(restZ.getzValue()).abs(), restZ.getName(),
                    Double.parseDouble(restZ.getLat()),
                    Double.parseDouble(restZ.getLon())));
        }

        Collections.sort(resultList, new Comparator<Result>() {
            @Override
            public int compare(Result o1, Result o2) {
                return o1.distance.compareTo(o2.distance);
            }
        });

        resultList = resultList.subList(0,k);
        String resString = "\n\nX: " + "23.0" + " Y: " + "24.0"+"\n";

        for (int v = 0; v < k; v++) {
            resString += resultList.get(v).restaurantName + " ....*******kNN Global " + resultList.get(v).distance + " x: "
                    + resultList.get(v).x + " y:" + resultList.get(v).y+"\n";
        }
        resString += "\n\n";
        System.err.println(resString);*/


        //X: 38.69 Y: 2.24
        double[] coord = new double[2];
        coord[0] = 38.69;
        coord[1] = 2.24;

        //43.6106519 y:7.017811
        double[] coord2 = new double[2];
        coord2[0] = 43.6241486;
        coord2[1] = 7.0034397;

       /* int[][] shiftvectors = new int[2][2];
        shiftvectors[0][0] = 0;
        shiftvectors[0][1] = 0;
        shiftvectors[1][0] = 2;
        shiftvectors[1][1] = 3;*/

        int[] converted = Zorder.convertCoord(1,2,1000,new int[2][2],coord);

        String zValue = Zorder.valueOf(2,converted);
        //String zValue = "1203";

        //zValue.charAt();
        /*int[] monEntier = new int[5];
        monEntier[0] = 4;
        monEntier[1] = 2;
        monEntier[2] = 1;
        monEntier[3] = 6;*/
       /* for(int i=0; i<converted.length; i++){
            System.out.println(converted[i]);
        }
        System.out.println(zValue);*/

        converted = Zorder.convertCoord(1,2,1000,new int[2][2],coord2);

        String zValue2 = Zorder.valueOf(2,converted);
        //System.out.println(zValue);
        /*char[] res = Zorder.fromStringToInt(zValue);
        for(int i=0; i<res.length; i++){
            System.out.print(res[i]);
        }*/
        BigInteger z = new BigInteger(String.valueOf(Zorder.fromStringToInt(zValue)));
        BigInteger v = new BigInteger(String.valueOf(Zorder.fromStringToInt(zValue2)));
        System.out.println(z.subtract(v).abs());
    }


}
