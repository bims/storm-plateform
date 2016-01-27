package tridentFunctions;

import inputClass.Input;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import otherClass.HBaseDB;
import otherClass.Restaurant;
import storm.trident.operation.Aggregator;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import tridentFunctions.InputCompareToDBFunction.Result;
import tridentFunctions.InputCompareToDBFunction.DistanceComparator;

import java.io.IOException;
import java.util.*;

/**
 * Created by asmaafillatre on 20/01/2016.
 */
public class ReducekNN implements Aggregator {

    List<Result> resultList = new ArrayList<>();
    @Override
    public Object init(Object o, TridentCollector tridentCollector) {
        return null;
    }

    @Override
    public void aggregate(Object o, TridentTuple input, TridentCollector tridentCollector) {
        //liste des bornes inf en static: 1, 185, 270, 356
        this.resultList = resultList;
        for(Result r : (List<Result>) input.getValueByField("Nimporte1")){
            resultList.add(r);
        }
        for(Result r : (List<Result>) input.getValueByField("Nimporte2")){
            resultList.add(r);
        }
        for(Result r : (List<Result>) input.getValueByField("Nimporte3")){
            resultList.add(r);
        }
        for(Result r : (List<Result>) input.getValueByField("Nimporte4")){
            resultList.add(r);
        }



        Collections.sort(resultList, new DistanceComparator());
        Input str = (Input) input.getValueByField("input");
        int k = 10;// # of neighbours

        System.err.println("\n\nX: " + str.getX() + " Y: " + str.getY());

        for (int v = 0; v < k; v++) {
            System.err.println(resultList.get(v).restaurantName + " ....*******kNN Global " + resultList.get(v).distance);
            //get classes of k nearest instances (city names) from the list into an array
            //ss[x] = resultList.get(x).restaurantName;
        }

        System.err.println("\n\n");
    }

    @Override
    public void complete(Object o, TridentCollector tridentCollector) {

    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {
        
    }

    @Override
    public void cleanup() {

    }

}







