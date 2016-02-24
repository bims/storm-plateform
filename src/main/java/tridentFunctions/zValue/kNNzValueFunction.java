package tridentFunctions.zValue;


import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class kNNzValueFunction extends BaseFunction {

    private int k;
    private int borneInf;
    private int borneSup;

    public kNNzValueFunction(int k, int borneInf, int borneSup){
        this.k = k;
        this.borneInf = borneInf;
        this.borneSup = borneSup;
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

    }
}
