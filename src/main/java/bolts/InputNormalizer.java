package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import inputClass.Input;


public class InputNormalizer extends BaseBasicBolt {

	public void cleanup() {}

	public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");

		String x = words[0];
		String y = words[1];

		Input obj = new Input(x,y);

		collector.emit(new Values(obj));

		//System.err.println("il y a des mots !!! x=" + x + " et y=" + y);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("inputcoord"));
	}
}
