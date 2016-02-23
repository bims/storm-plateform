package testTridentFunctions;

import backtype.storm.tuple.Values;
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



public class testFunction2 extends BaseFunction {

    private int partitionIndex;
    private String msg;

    public testFunction2(String msg){
        this.msg = msg;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.partitionIndex = context.getPartitionIndex();
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
        System.out.println(msg+" Partition "+partitionIndex+ " voici mon tuple : "+tuple.getString(0)+" "+tuple.getString(1));
        collector.emit(tuple);
    }
}
