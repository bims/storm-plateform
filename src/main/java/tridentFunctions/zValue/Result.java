package tridentFunctions.zValue;

import java.math.BigInteger;

//simple class to model results (distance + class)
public class Result {
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
