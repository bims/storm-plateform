package testTridentFunctions;

        import backtype.storm.tuple.Values;
        import inputClass.Input;
        import inputClass.InputZValue;
        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.hbase.HBaseConfiguration;
        import otherClass.HBaseDB;
        import otherClass.Restaurant;
        import storm.trident.operation.BaseFunction;
        import storm.trident.operation.TridentCollector;
        import storm.trident.operation.TridentOperationContext;
        import storm.trident.tuple.TridentTuple;

        import java.io.IOException;
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
            if(msg.equals(">1")){
                for(int i=0; i<100000;i++){
                    double r = Math.pow(10.0,4.0);
                }
            }
            System.out.println(msg+" Partition "+partitionIndex+ " voici mon tuple : "+ inpZ.getzValue());
            collector.emit(new Values(tuple.getValue(0)+msg));
    }
}