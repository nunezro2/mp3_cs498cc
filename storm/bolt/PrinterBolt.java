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
    	// THIS PART CAN BE REMOVED... IT'S JUST A GUIDE
    	String output = "source " + tuple.getSourceComponent() + ", ";
    	output += "noFields " + tuple.size() + ", ";
    	output += "fieldsName " + tuple.getFields() + ", ";
    	output += "getSourceGlobalStreamid " + tuple.getSourceGlobalStreamid() + ", ";
    	output += "id " + tuple.getSourceStreamId() + ", ";
    	output += " " + tuple.getValues();
    	
    	//collector.emit(new Values("OUTPUT TUPLE: " + tuple))
    	System.out.println("OUTPUT TUPLE: " + tuple.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
    
}