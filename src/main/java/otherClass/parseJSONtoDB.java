package otherClass;

import org.apache.storm.shade.org.json.simple.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pb402 on 25/11/2015.
 */
public class parseJSONtoDB {



    public static void main(String[] args){

        //on cree un objet json pour récupérer le résultat.
        JSONObject obj = new JSONObject();

        //String pour recuperer la string du fichier
        String stringDB = "";

        //on lit le fichier
        Path path_DataBase = Paths.get("src/main/resources", "restaurant.json");

        byte[] ArrayDB = new byte[0];
        try {
            ArrayDB = Files.readAllBytes(path_DataBase);
            stringDB = new String(ArrayDB, "ISO-8859-1");
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println(stringDB);


    }
}
