package testTridentFunctions;

import backtype.storm.tuple.Values;
import org.apache.commons.collections.MapUtils;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sy306571 on 21/02/16.
 */
public class TestAggregator extends BaseAggregator<Map<String, Integer>> {

    @Override
    public Map<String, Integer> init(Object batchId, TridentCollector collector) {
        return new HashMap<String, Integer>();
    }

    @Override
    public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
        //String location = tuple.getString(0);
        val.put("nombre", MapUtils.getInteger(val, "nombre", 0) + 1);
    }

    @Override
    public void complete(Map<String, Integer> val, TridentCollector collector) {
        collector.emit(new Values(val));
    }
}
