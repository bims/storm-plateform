package tridentFunctions.zValue;


import backtype.storm.tuple.Values;
import inputClass.Input;
import inputClass.InputZValue;
import scala.math.Ordering;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.math.BigInteger;
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
        val.add(new InputZValue((Input) input.getValue(0),(BigInteger) input.getValue(1)));
    }

    @Override
    public void complete(List<InputZValue> val, TridentCollector tridentCollector) {
        Collections.sort(val, new Comparator<InputZValue>() {
            @Override
            public int compare(final InputZValue o1, final InputZValue o2) {
                return o1.getzValue().compareTo(o2.getzValue());
            }
        });

        int nbTuples[] = new int[nbParts];
        int remaining = val.size();
        int j=0;
        while(remaining > 0){
            nbTuples[j]++;
            remaining--;
            j++;
            if(j==nbParts)
                j=0;
        }

        int compt = 0;
        for(int i=0; i<nbParts; i++){
            for(j=0; j<nbTuples[i]; j++){
                tridentCollector.emit(new Values(val.get(compt),i));
                compt++;
            }
        }
    }

}
