package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.bolt.JoinBolt;
import storm.starter.bolt.*;

import java.util.Random;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import storm.starter.util.TupleHelpers;

public class JoinAndFilterTopology {

    public static class GeoLocationCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();
        Fields _outFields;

        public GeoLocationCount(Fields outFields) {
            _outFields = outFields;
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            if (TupleHelpers.isTickTuple(tuple)) {
                for(Map.Entry<String, Integer> entry : counts.entrySet()) {
                    String geo_location = entry.getKey();
                    Integer count = entry.getValue();
                    collector.emit(new Values(geo_location, count));
                }
            }
            else {
                int retweets = tuple.getInteger(1);
                int likes = tuple.getInteger(2);
                String geo_location = tuple.getString(3);
                Integer count = counts.get(geo_location);
                if(count==null) count = 0;
                count += retweets + likes;
                counts.put(geo_location, count);
                //collector.emit(new Values(geo_location, count));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(_outFields);
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            Map<String, Object> conf = new HashMap<String, Object>();
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
            return conf;
        }
    }

    public static void main(String[] args) {
        
        FeederSpout facebookSpout = new FeederSpout(new Fields("id", "likes", "geo_location"));
        FeederSpout twitterSpout = new FeederSpout(new Fields("id", "retweets"));
        
        // Your topology goes here.
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("facebook", facebookSpout);
        builder.setSpout("twitter", twitterSpout);
        // you need to extend this topology in order to Join and Filter the two streams
        
        builder.setBolt("join", new JoinBolt(new Fields("id", "retweets", "likes", "geo_location")))
                .fieldsGrouping("facebook", new Fields("id"))
                .fieldsGrouping("twitter", new Fields("id"));
        // Add a filter bolt to your topology to filter out any message that has retweets less than 4 or likes less than 8. 
        
        builder.setBolt("filter", new FilterBolt(new Fields("id", "retweets", "likes", "geo_location"))).shuffleGrouping("join");

        builder.setBolt("count", new GeoLocationCount(new Fields("geo_location", "count"))).fieldsGrouping("filter", new Fields("geo_location"));
        // Add another bolt to keep count of total likes and retweets per message
        
        
        // PrinterBolt finally prints out the end result to System.out
        // This assumes that the bolt that feeds into it was named "filter",
        // change as per your topology.
        
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("count");
        // builder.setBolt("print", new PrinterBolt()).shuffleGrouping("join");
        
        
        
        Config conf = new Config();
        conf.setDebug(true);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("join-and-filter-example", conf, builder.createTopology());
        
        
        Random generator = new Random();
        
        String geo_location = "";
        int region;
        // for(int i=0; i<10000000; i++) {
        for(int i=0; i<20; i++) {
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
                
        Utils.sleep(25000);
        cluster.shutdown();
    }
}