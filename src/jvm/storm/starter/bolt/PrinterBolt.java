package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;

public class PrinterBolt extends BaseBasicBolt{

    String _filename;

    public PrinterBolt(String filename) {
        _filename = filename;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            PrintWriter out = new PrintWriter(new FileWriter(_filename, true));
            out.println("OUTPUT TUPLE: " + tuple);
            out.close();
        }catch(java.io.IOException ex){
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
    
}
