package org.storm.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.lossy.LossyCounter;

import java.io.IOException;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class AggregatorBolt extends BaseBasicBolt {
    Logger logger;
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        // Initialize logger
        logger = Logger.getLogger(AggregatorBolt.class.getName());
        try {
            // Create file handler
            FileHandler fh = new FileHandler("current_cloud.log");
            // Set formatter
            fh.setFormatter(new SimpleFormatter());
            // Add file handler to logger
            logger.addHandler(fh);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public synchronized void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String out = (String) tuple.getValue(0);
        System.out.println(out);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
