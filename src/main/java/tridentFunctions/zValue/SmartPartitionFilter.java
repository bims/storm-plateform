package tridentFunctions.zValue;


import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class SmartPartitionFilter extends BaseFilter {

    private int numPart;

    public SmartPartitionFilter(int numPart){
        this.numPart = numPart;
    }

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        return ((Integer) tridentTuple.getValueByField("numPartition"))==numPart;
    }
}
