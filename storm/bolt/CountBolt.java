package bolt;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.Constants;
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
	private static final int EMIT_FREQ_SEC = 5;
	 
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) 
    	{
    		
            double totalCount = 0.0;
            for (int regCounter: counts.values()) {
            	totalCount += regCounter;
            }
    		
            String output = new String();
            for (String reg: counts.keySet()) {
            	output += (reg + ", " + Math.round(((counts.get(reg)/totalCount)*1000))/10.0 + "% \n" );
            }
            
            collector.emit(new Values(output));
    	} 
    	else 
    	{
    		String region = tuple.getStringByField("geo_location");
            Integer count = counts.get(region);
            if(count==null) count = 0;
            count += tuple.getIntegerByField("retweets") + tuple.getIntegerByField("likes");
            counts.put(region, count);           
    	}
    	
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    	ofd.declare(new Fields("percentages"));
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, EMIT_FREQ_SEC);
        return conf;
    }
    
}