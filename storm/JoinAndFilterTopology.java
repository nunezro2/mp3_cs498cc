
import bolt.JoinBolt;
import bolt.PrinterBolt;
import bolt.FilterBolt;
import bolt.CountBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Random;

public class JoinAndFilterTopology {
    public static void main(String[] args) {
    	
        FeederSpout facebookSpout = new FeederSpout(new Fields("id", "likes", "geo_location"));
        FeederSpout twitterSpout = new FeederSpout(new Fields("id", "retweets"));
        
        // Your topology goes here.
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("facebook", facebookSpout);
        builder.setSpout("twitter", twitterSpout);

        // you need to extend this topology in order to Join and Filter the two streams
        
        builder.setBolt("join", new JoinBolt(new Fields("id", "retweets", "likes", "geo_location")), 8)
                .fieldsGrouping("facebook", new Fields("id"))
                .fieldsGrouping("twitter", new Fields("id"));
        // Add a filter bolt to your topology to filter out any message that has retweets less than 4 or likes less than 8. 
        builder.setBolt("filter", new FilterBolt(), 8).shuffleGrouping("join");
        // Add another bolt to keep count of total likes and retweets per message
        builder.setBolt("counter", new CountBolt()).globalGrouping("filter");
        // PrinterBolt finally prints out the end result to System.out
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("counter");
        
        Config conf = new Config();
        conf.setDebug(false);   //////   <----- changed to false
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("join-and-filter-example", conf, builder.createTopology());
        
        
        Random generator = new Random();
        
        String geo_location = new String();
        for(int i=0; i < 10000000; i++) {    ///  This should be 10000000, I changed it for testing
            twitterSpout.feed(new Values(i, generator.nextInt(10 * ((i%3)+1))));
            if(i % 3 == 0) {
                geo_location = "Asia/Pacific";
            } else if(i % 3 == 1) {
                geo_location = "Europe";
            }
            else
            {
            	geo_location = "US";
            }
            facebookSpout.feed(new Values(i, generator.nextInt(25 * ((i%3)+1)), geo_location));
        }
                
        Utils.sleep(60000);
        cluster.shutdown();
    }
}