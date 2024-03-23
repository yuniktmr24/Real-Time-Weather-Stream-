package org.storm.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.lossy.LossyCounter;

import java.io.IOException;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class TopKCloudyPrinterBolt extends BaseBasicBolt {
    Logger logger;
    BasicOutputCollector outputCollector;
    LossyCounter counter;
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        // Initialize logger
        logger = Logger.getLogger(TopKCloudyPrinterBolt.class.getName());
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
        if (outputCollector == null) {
            outputCollector = basicOutputCollector;
        }
        counter = (LossyCounter) tuple.getValue(0);
        logger.info(counter.toString());
        //String counterStr = counter.toString();
        //System.out.println(counter.toString());
        //outputCollector.emit(new Values(counterStr));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("printOut"));
    }
}
