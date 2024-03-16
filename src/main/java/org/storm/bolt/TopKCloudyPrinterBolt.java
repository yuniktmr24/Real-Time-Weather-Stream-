package org.storm.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class TopKCloudyPrinterBolt extends BaseBasicBolt {
    Logger logger;
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        // Initialize logger
        logger = Logger.getLogger(TopKCloudyBolt.class.getName());
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
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
       StringBuilder out = (StringBuilder) tuple.getValue(0);
       logger.info(out.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
