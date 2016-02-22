package tridentFunctions;

import backtype.storm.tuple.Values;
import inputClass.Input;
import org.apache.commons.collections.MapUtils;
import storm.trident.operation.Aggregator;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import tridentFunctions.InputCompareToDBFunction.Result;

import java.util.*;

/**
 * Created by asmaafillatre on 20/01/2016.
 */
public class ReducekNNAggregator extends BaseAggregator<Map<String, Integer>> {

    List<Result> resultList;
    @Override
    public Map<String,Integer> init(Object o, TridentCollector tridentCollector) {

        return new HashMap<String,Integer>();
    }

    @Override
    public void aggregate(Map<String, Integer> val, TridentTuple input, TridentCollector tridentCollector) {
        //liste des bornes inf en static: 1, 185, 270, 356
        resultList = new ArrayList<>();
        if(input.getValueByField("Nimporte1")!=null){
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



            Collections.sort(resultList, new InputCompareToDBFunction.DistanceComparator());
            Input str = (Input) input.getValueByField("input");
            int k = 10;// # of neighbours

            System.err.println("\n\nX: " + str.getX() + " Y: " + str.getY());

            for (int v = 0; v < k; v++) {
                System.err.println(resultList.get(v).restaurantName + " ....*******kNN Global " + resultList.get(v).distance+ " x: "+resultList.get(v).x+" y:"+resultList.get(v).y);
                //get classes of k nearest instances (city names) from the list into an array
                //ss[x] = resultList.get(x).restaurantName;
            }

            System.err.println("\n\n");
        }
    }

    @Override
    public void complete(Map<String, Integer> val, TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(val));
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {
        
    }

    @Override
    public void cleanup() {
    }

}







