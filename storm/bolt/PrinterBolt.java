package bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class PrinterBolt extends BaseBasicBolt {

	
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

    	if( tuple.contains("percentages")  &&  !tuple.getStringByField("percentages").isEmpty())
    		System.out.println(tuple.getStringByField("percentages"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
    
}