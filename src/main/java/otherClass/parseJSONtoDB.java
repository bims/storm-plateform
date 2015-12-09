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

/**
 * Created by pb402 on 25/11/2015.
 */

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
        config.addResource(new org.apache.hadoop.fs.Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));

        HBaseDB restaurantsDB = new HBaseDB(config);

        for(Restaurant rest: allRestaurants) {
            System.out.println("Rest[" + rest.getId() + "]:(" + rest.getLat() + "," + rest.getLon() + ")");
            restaurantsDB.addItem(rest.getId(), rest.getLat(), rest.getLon(), rest.getTags().getName(), rest.getTags().getAddrStreet());
        }

        restaurantsDB.GetRow("277052529");
    }
}
