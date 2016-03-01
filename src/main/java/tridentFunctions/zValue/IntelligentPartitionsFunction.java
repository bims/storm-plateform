package tridentFunctions.zValue;

import inputClass.InputZValue;
import otherClass.ZLimits;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;

public class IntelligentPartitionsFunction extends BaseFunction {

    private HashMap<Integer, ZLimits> zLimits;

    public IntelligentPartitionsFunction(HashMap<Integer, ZLimits> zLimits){
        this.zLimits = zLimits;
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        //InputZValue inp = (InputZValue) tridentTuple.getValue(0);
        System.out.println(zLimits.get(0));
        System.out.println(zLimits.get(1));
        System.out.println(zLimits.get(2));
        System.out.println(zLimits.get(3));
        System.out.println("#####################");
    }

    public HashMap<Integer, ZLimits> getzLimits() {
        return zLimits;
    }
}
