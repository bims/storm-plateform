package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import inputClass.Input;

import java.util.Map;


public class InputCompareToDB extends BaseBasicBolt {

	Integer id;
	String name;


	@Override
	public void cleanup() {
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	public void execute(Tuple input, BasicOutputCollector collector) {

		Input str = (Input) input.getValue(0);
		System.err.println("X: " + str.getX() + " Y: " + str.getY());

	}


}
