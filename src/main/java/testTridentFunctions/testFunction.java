package testTridentFunctions;

        import backtype.storm.tuple.Values;
        import inputClass.InputZValue;
        import storm.trident.operation.BaseFunction;
        import storm.trident.operation.TridentCollector;
        import storm.trident.operation.TridentOperationContext;
        import storm.trident.tuple.TridentTuple;

        import java.util.*;

/**
 * Created by asmaafillatre on 25/01/2016.
 */


public class testFunction extends BaseFunction {

    private int partitionIndex;
    private String msg;

    public testFunction(String msg){
        this.msg = msg;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.partitionIndex = context.getPartitionIndex();
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
            InputZValue inpZ = (InputZValue) tuple.getValue(0);


            System.out.println(msg+" Partition "+partitionIndex+ " voici mon tuple : "+ inpZ.getzValue());
            collector.emit(new Values(tuple.getValue(0)));
    }
}