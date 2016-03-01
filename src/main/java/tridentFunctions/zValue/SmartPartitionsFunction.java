package tridentFunctions.zValue;

import otherClass.ZLimits;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.List;

public class SmartPartitionsFunction extends BaseFunction {

    private HashMap<Integer,ZLimits> zLimits;

    public SmartPartitionsFunction(HashMap<Integer,ZLimits> zLimits){
        this.zLimits = zLimits;
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        //InputZValue inp = (InputZValue) tridentTuple.getValue(0);

        System.out.println("#####################");
    }

    public HashMap<Integer,ZLimits> getzLimits() {
        return zLimits;
    }
}
