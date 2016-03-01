package tridentFunctions.zValue;

import inputClass.Input;
import inputClass.InputZValue;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import tridentFunctions.InputCompareToDBFunction;

import java.util.*;


public class ReducezkNNFunction extends BaseFunction{

    private int partitionIndex;
    private int nbParts;
    private int k;

    public ReducezkNNFunction(int k, int nbParts){
        this.nbParts = nbParts;
        this.k = k;
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
        Collections.sort(resultList, new Comparator<Result>() {
            @Override
            public int compare(Result o1, Result o2) {
                return o1.distance.compareTo(o2.distance);
            }
        });

        InputZValue str = (InputZValue) input.getValueByField("inputZValue");
        int k = this.k;// # of neighbours

        String resString = "\n\nPartition "+partitionIndex+"\nX:" + str.getX() + " Y:" + str.getY()+"\n";

        for (int v = 0; v < k; v++) {
            resString += resultList.get(v).restaurantName + " ....*******kNN Global " + resultList.get(v).distance + " x: " + resultList.get(v).x + " y:" + resultList.get(v).y+"\n";
            //get classes of k nearest instances (city names) from the list into an array
            //ss[x] = resultList.get(x).restaurantName;
        }
        resString += "\n\n";
        System.err.println(resString);

    }
}
