package tridentFunctions.zValue;


import backtype.storm.tuple.Values;
import inputClass.Input;
import inputClass.InputZValue;
import scala.math.Ordering;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.*;

public class SortAggregator extends BaseAggregator<List<InputZValue>> {
    private int nbParts;

    public SortAggregator(int nbParts){
        this.nbParts = nbParts;
    }

    @Override
    public List<InputZValue> init(Object o, TridentCollector tridentCollector) {

        return new ArrayList<InputZValue>();
    }

    @Override
    public void aggregate(List<InputZValue> val, TridentTuple input, TridentCollector tridentCollector) {
        val.add(new InputZValue((Input) input.getValue(0),input.getDouble(1)));
    }

    @Override
    public void complete(List<InputZValue> val, TridentCollector tridentCollector) {
        Collections.sort(val, new Comparator<InputZValue>() {
            @Override
            public int compare(final InputZValue o1, final InputZValue o2) {
                return Double.compare(o1.getzValue(),o2.getzValue());
            }
        });
        long nbTuplesByPartition = Math.round(((double) val.size())/((double) nbParts));
        int numPartition = 1;
        for(int i=0; i<val.size(); i++){
            if(i!=0 && numPartition<nbParts && i%nbTuplesByPartition==0){
                numPartition++;
            }
            tridentCollector.emit(new Values(val.get(i),numPartition));
        }
    }

}
