package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Created by sy306571 on 02/12/15.
 */
public class TestKafka extends BaseBasicBolt {
    public void cleanup() {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        System.out.println(sentence);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
