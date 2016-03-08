package tridentFunctions.zValue;

import backtype.storm.tuple.Values;
import inputClass.Input;
import inputClass.InputZValue;
import otherClass.ZLimits;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.math.BigInteger;
import java.util.HashMap;

public class SmartPartitionsFunction extends BaseFunction {

    private HashMap<Integer, ZLimits> zLimits;
    private int nbParts;

    public SmartPartitionsFunction(HashMap<Integer,ZLimits> zLimits, int nbParts){
        this.zLimits = zLimits;
        this.nbParts = nbParts;
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        Input input = (Input) tridentTuple.getValue(0);
        BigInteger zValue = (BigInteger) tridentTuple.getValue(1);
        InputZValue inp = new InputZValue(input,zValue);


        ZLimits zlInf = zLimits.get(0);
        ZLimits zlSup = zLimits.get(nbParts-1);
        if(zlInf.getBorneInf().compareTo(inp.getzValue())>0){
            tridentCollector.emit(new Values(inp,0));
        }
        else if(zlSup.getBorneSup().compareTo(inp.getzValue())<0){
            tridentCollector.emit(new Values(inp,nbParts-1));
        }
        else{
            for(int i=0; i<nbParts; i++){
                ZLimits zl = zLimits.get(i);
                if(zl.getBorneInf().compareTo(inp.getzValue())<=0 && zl.getBorneSup().compareTo(inp.getzValue())>=0){
                    tridentCollector.emit(new Values(inp,i));
                }
            }
        }
    }

}
