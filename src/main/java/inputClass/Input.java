package inputClass;

/**
 * Created by pb402 on 24/11/2015.
 */
public class Input {

    //Les coordonnées des données entrées en input
    String x;
    String y;

    //constructeur
    public Input(String x, String y){
        this.x = x;
        this.y = y;
    }


    //getters pour recupérer x et y
    public String getX(){
        return this.x;
    }

    public String getY(){
        return this.y;
    }

}
