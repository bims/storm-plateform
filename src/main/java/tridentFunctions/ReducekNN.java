package tridentFunctions;

import inputClass.Input;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import otherClass.HBaseDB;
import otherClass.Restaurant;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.*;

/**
 * Created by asmaafillatre on 20/01/2016.
 */
public class ReducekNN extends BaseFunction {

    List<Result> resultList = new ArrayList<Result>();

    //public ReducekNN(TridentCollector collector){
        public void execute(TridentTuple input, TridentCollector collector) {
        //liste des bornes inf en static: 1, 185, 270, 356
        this.resultList = resultList;

            Collections.sort(resultList, new DistanceComparator());
            Input str = (Input) input.getValue(0);
        int k = 10;// # of neighbours

            System.err.println("\n\nX: " + str.getX() + " Y: " + str.getY());

            for (int v = 0; v < k; v++) {
                System.err.println(resultList.get(v).restaurantName + " ....*******ASmaa " + resultList.get(v).distance);
                //get classes of k nearest instances (city names) from the list into an array
                //ss[x] = resultList.get(x).restaurantName;
            }

            System.err.println("\n\n");
    }



    //simple class to model results (distance + class)
    static class Result {
        double distance;
        String restaurantName;
        public Result(double distance, String restaurantName){
            this.restaurantName = restaurantName;
            this.distance = distance;
        }
    }

    static class DistanceComparator implements Comparator<Result> {
        //@Override
        public int compare(Result a, Result b) {
            return a.distance < b.distance ? -1 : a.distance == b.distance ? 0 : 1;
        }
    }
}







