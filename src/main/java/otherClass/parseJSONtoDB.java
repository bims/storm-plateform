package otherClass;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.shade.com.google.common.reflect.TypeToken;
import org.apache.storm.shade.org.apache.commons.io.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class parseJSONtoDB {

    private static final int ZVALUE_MODE = 1;
    private static final int NB_PARTITIONS = 5;


    public static void main(String[] args) throws IOException {

        int mode = 0;
        if(args.length > 0){
            mode = Integer.parseInt(args[0]);
        }

        // String pour recuperer la string du fichier
        String stringDB = "";

        // On lit le fichier
        String path_DataBase = "/src/main/resources/restaurant.json";
        InputStream inputDB = parseJSONtoDB.class.getResourceAsStream("/src/main/resources/restaurant.json");
        if (inputDB == null) {
            throw new FileNotFoundException("File " + path_DataBase + " does not exist");
        }

        try {
            byte[] ArrayDB = IOUtils.toByteArray(inputDB);
            stringDB = new String(ArrayDB, "ISO-8859-1");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Deserialize the list of Restaurants from JSon to Java Restaurant Objects.
        Gson gson = new Gson();

        Type listType = new TypeToken<ArrayList<Restaurant>>(){}.getType();

        List<Restaurant> allRestaurants = gson.fromJson(stringDB, listType);
        List<RestaurantZValue> restaurantZValues = new ArrayList<>();

        Configuration config = HBaseConfiguration.create();
        //config.addResource("hbase-site.xml");
        config.set("hbase.zookeeper.quorum", "localhost:2181");  // Here we are running zookeeper locally


        HBaseDB restaurantsDB = new HBaseDB(config);
        restaurantsDB.DropTable();
        restaurantsDB.CreateTable();

        int cpt = 0;
        for(Restaurant rest: allRestaurants) {

            cpt++;

            //System.out.println("Numéro "+cpt+":  Rest[" + rest.getId() + "]:(" + rest.getLat() + "," + rest.getLon() + ")");
            String id = Integer.toString(cpt);
            String lat = rest.getLat();
            String lon = rest.getLon();

            String name = "N/C";
            if(rest.getTags().getName() != null){
                name = rest.getTags().getName();
            }

            //String addr = rest.getTags().getAddrStreet() == null? "":rest.getTags().getAddrStreet();
            //restaurantsDB.addItem(id, lat, lon, name, addr);
            if(mode == ZVALUE_MODE){
                double[] coord = new double[2];
                coord[0] = Double.parseDouble(rest.getLat());
                coord[1] = Double.parseDouble(rest.getLon());

                //int[] zValue = data_preprocessing.Convertcoord(2,coord);
                double zValue = (coord[0] + coord[1])*((coord[0] + coord[1]));

                restaurantZValues.add(new RestaurantZValue(id,lat,lon,name,zValue));
            }
            else restaurantsDB.addItem(id, lat, lon, name);
        }

        if(mode == ZVALUE_MODE){
            Collections.sort(restaurantZValues, new Comparator<RestaurantZValue>() {

                @Override
                public int compare(RestaurantZValue o1, RestaurantZValue o2) {
                    return Double.compare(o1.getzValue(), o2.getzValue());
                }
            });

            long nbTuplesByPartition = Math.round(((double) restaurantZValues.size())/((double) NB_PARTITIONS));
            int numPartition = 1;
            for(int i=0; i<restaurantZValues.size(); i++) {
                if (i != 0 && numPartition < NB_PARTITIONS && i % nbTuplesByPartition == 0) {
                    numPartition++;
                }
                RestaurantZValue rest = restaurantZValues.get(i);
                restaurantsDB.addItemZValue(zeroPadding(String.valueOf(i)),rest.getLat(),rest.getLon(),rest.getName(),
                        rest.getzValue(),numPartition-1);
            }
        }

        //DEBUG
        //System.out.println("ROWWWWWWWWWWWWWWWWWW");
        //restaurantsDB.GetRow("1");
        //System.out.println("ROWWWWWWWWWWWWWWWWWW");
        //restaurantsDB.GetRow("2");
    }


    //Faire une fonction de ce genre pour remplacer les problèmes d'accents
    //Г§ = ç
    //Г© = é
    //ГЁ = ê
    //Г = à
    //etc...
    public static String sansAccent(String s) {
        final String accents = "ÀÁÂÃÄÅàáâãäåÈÉÊËèéêë"; // A compléter...
        final String letters = "AAAAAAaaaaaaEEEEeeee"; // A compléter...

        StringBuffer buffer = null;
        for(int i=s.length()-1 ; i>=0; i--) {
            int index = accents.indexOf(s.charAt(i));
            if (index>=0) {
                if (buffer==null) {
                    buffer = new StringBuffer(s);
                }
                buffer.setCharAt(i, letters.charAt(index));
            }
        }
        return buffer==null ? s : buffer.toString();
    }

    private static String zeroPadding(String num){
        StringBuilder padded = new StringBuilder("0000000000");

        for(int i = 0; i < num.length(); i++){
            padded.setCharAt(padded.length() - (i + 1), num.charAt(num.length() - i - 1));
        }

        return padded.toString();
    }

    private static class RestaurantZValue{
        private String id;
        private String lat;
        private String lon;
        private String name;
        private double zValue;

        public RestaurantZValue(String id, String lat, String lon, String name, double zValue){
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

        public double getzValue() {
            return zValue;
        }

        public String getName() {
            return name;
        }

    }
}
