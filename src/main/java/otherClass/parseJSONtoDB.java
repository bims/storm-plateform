package otherClass;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.shade.com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


public class parseJSONtoDB {

    public static void main(String[] args) throws IOException {

        // String pour recuperer la string du fichier
        String stringDB = "";

        // On lit le fichier
        Path path_DataBase = Paths.get("src/main/resources", "restaurant.json");

        try {
            byte[] ArrayDB = Files.readAllBytes(path_DataBase);
            stringDB = new String(ArrayDB, "ISO-8859-1");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Deserialize the list of Restaurants from JSon to Java Restaurant Objects.
        Gson gson = new Gson();

        Type listType = new TypeToken<ArrayList<Restaurant>>(){}.getType();

        List<Restaurant> allRestaurants = gson.fromJson(stringDB, listType);

        Configuration config = HBaseConfiguration.create();
        //config.addResource("hbase-site.xml");

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
            restaurantsDB.addItem(id, lat, lon, name);
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
}
