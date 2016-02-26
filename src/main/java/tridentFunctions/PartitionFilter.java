package tridentFunctions;


import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class PartitionFilter extends BaseFilter{

    private int partitionIndex;
    private int partitionS;

    public PartitionFilter(int partitionS){
        this.partitionS = partitionS;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.partitionIndex = context.getPartitionIndex();
    }

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        return partitionIndex==partitionS;
    }
}
