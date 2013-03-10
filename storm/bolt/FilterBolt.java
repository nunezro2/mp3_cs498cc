package bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class FilterBolt extends BaseBasicBolt {
	
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	
    	if ( (tuple.getIntegerByField("retweets")) >= 4 && (tuple.getIntegerByField("likes")) >= 8 ) 
    	{
    		collector.emit(new Values(tuple.getValue(0), tuple.getValue(1), tuple.getValue(2), tuple.getValue(3)));
    	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    	ofd.declare(new Fields("id", "retweets", "likes", "geo_location"));
    }
    
}