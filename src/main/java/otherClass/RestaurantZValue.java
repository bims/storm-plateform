package otherClass;

import java.math.BigInteger;

/**
 * Created by sy306571 on 01/03/16.
 */
public class RestaurantZValue extends Restaurant implements Comparable<RestaurantZValue>{
    private String id;
    private String lat;
    private String lon;
    private String name;
    private BigInteger zValue;

    public RestaurantZValue(){

    }

    public RestaurantZValue(String id, String lat, String lon, String name, BigInteger zValue){
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.zValue = zValue;
        this.name = name;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setzValue(BigInteger zValue) {
        this.zValue = zValue;
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
