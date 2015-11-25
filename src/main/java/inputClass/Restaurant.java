package inputClass;

/**
 * Created by pb402 on 25/11/2015.
 */
public class Restaurant {

    String id;
    String xCor;
    String yCor;
    String name;

    //constructeur
    public Restaurant(String id, String x, String y){
        this.id = id;
        this.xCor = x;
        this.yCor = y;
    }

    public void setName(String name){
        this.name = name;
    }

    public String getName(){
        return this.name;
    }

    public String getId(){
        return this.id;
    }

    public String getXcor(){
        return this.xCor;
    }

    public String getYcor(){
        return this.yCor;
    }
}
