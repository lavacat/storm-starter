package storm.starter.bolt;

import backtype.storm.Config;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import storm.starter.util.TupleHelpers;

public class GeoLocationCountBolt extends BaseRichBolt{
    OutputCollector _collector;
    Map<String, Integer> counts;
    Integer total;
    Fields _outFields;

    public GeoLocationCountBolt(Fields outFields) {
        _outFields = outFields;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        counts = new HashMap<String, Integer>();
        total = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            for(Map.Entry<String, Integer> entry : counts.entrySet()) {
                String geo_location = entry.getKey();
                Integer count = entry.getValue();
                _collector.emit(new Values(geo_location, "" + (count*100./total) + "%"));
            }
        }
        else {
            int retweets = tuple.getInteger(1);
            int likes = tuple.getInteger(2);
            String geo_location = tuple.getString(3);
            Integer count = counts.get(geo_location);
            if(count==null) count = 0;
            count += retweets + likes;
            total += retweets + likes;
            counts.put(geo_location, count);
            _collector.ack(tuple);
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