package bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class CountBolt extends BaseBasicBolt {
	Map<String, Integer> counts = new HashMap<String, Integer>();
	double globalCount = 0.0;
	 
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String region = tuple.getStringByField("geo_location");
        Integer count = counts.get(region);
        if(count==null) count = 0;
        count += tuple.getIntegerByField("retweets") + tuple.getIntegerByField("likes");
        counts.put(region, count);
        globalCount += count;
        
        String output = new String();
        for (String reg: counts.keySet()) {
        	output += (reg + " " + (counts.get(reg)/globalCount)*100 + " % \n" );
        	//System.out.print( "\n" + reg + "\n");
        }
        
        collector.emit(new Values(output));
    	
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    	//ofd.declare(new Fields("id", "retweets", "likes", "geo_location"));
    	ofd.declare(new Fields("percentages"));
    	
    }
    
}