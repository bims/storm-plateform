package otherClass;


import com.google.gson.Gson;
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

    public static void main(String[] args){

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

        for(Restaurant rest: allRestaurants){
            System.out.println("Rest[" + rest.getId() + "]:(" + rest.getLat() + "," + rest.getLon() + ")");
        }

    }
}
