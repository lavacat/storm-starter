package storm.starter.bolt;

import java.util.List;
import java.util.ArrayList;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FilterBolt extends BaseBasicBolt {

    Fields _outFields;
    int _retweetMin;
    int _likesMin;

    public FilterBolt(Fields outFields, int retweetMin, int likesMin) {
        _outFields = outFields;
        _retweetMin = retweetMin;
        _likesMin = likesMin;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // System.out.println("OUTPUT TUPLE:" + tuple);

	int ID = tuple.getInteger(0);
	int retweets = tuple.getInteger(1);
	int likes = tuple.getInteger(2);
	String geo_location = tuple.getString(3);

	if (retweets >= _retweetMin && likes >= _likesMin)
		collector.emit(new Values(ID, retweets, likes, geo_location));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
	ofd.declare(_outFields);
    }
    
}
