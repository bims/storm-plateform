package tridentFunctions;

import backtype.storm.tuple.Values;
import inputClass.Input;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import otherClass.HBaseDB;
import otherClass.Restaurant;
import storm.trident.operation.*;
import storm.trident.tuple.TridentTuple;
import tridentFunctions.InputCompareToDBFunction.Result;
import tridentFunctions.InputCompareToDBFunction.DistanceComparator;

import java.io.IOException;
import java.util.*;

/**
 * Created by asmaafillatre on 20/01/2016.
 */
public class ReducekNNFunction extends BaseFunction {

    private int partitionIndex;
    private int nbParts;

    public ReducekNNFunction(int nbParts){
        this.nbParts = nbParts;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.partitionIndex = context.getPartitionIndex();
    }

    @Override
    public void execute(TridentTuple input, TridentCollector collector) {
        //liste des bornes inf en static: 1, 185, 270, 356
        List<Result> resultList = new ArrayList<Result>();

        for(int i=0; i<nbParts;i++){
            for (Result r : (List<Result>) input.getValueByField("Partition S"+i)) {
                resultList.add(r);
            }
        }

        //On applique le kNN global
        Collections.sort(resultList, new DistanceComparator());
        Input str = (Input) input.getValueByField("input");
        int k = 10;// # of neighbours

        System.err.println("\n\nPartition "+partitionIndex+"\nX: " + str.getX() + " Y: " + str.getY());

        for (int v = 0; v < k; v++) {
            System.err.println(resultList.get(v).restaurantName + " ....*******kNN Global " + resultList.get(v).distance + " x: " + resultList.get(v).x + " y:" + resultList.get(v).y);
            //get classes of k nearest instances (city names) from the list into an array
            //ss[x] = resultList.get(x).restaurantName;
        }

        System.err.println("\n\n");

    }

}
