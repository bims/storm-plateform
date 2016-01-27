package tridentFunctions;

        import inputClass.Input;
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
/*

public class testFunction extends BaseFunction {
    public void execute(TridentTuple tuple, TridentCollector collector) {
        for(int i=0; i < tuple.getInteger(0); i++) {
            collector.emit(new Values(i));
        }
    }
}
*/