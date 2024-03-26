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
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TopKCloudyPrinterBolt extends BaseBasicBolt {
    Logger logger;
    BasicOutputCollector outputCollector;

    List<String> cloudData = new ArrayList<>();

    Map <String, String> cloudMap = new TreeMap();

    int outputCounter;
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
        String out = tuple.getStringByField("cloud_cover_data");
        cloudData.add(out);

        outputCounter++;
        //counter = (LossyCounter) tuple.getValue(0);
        if (outputCounter % 5 == 0) {
            for (String data: cloudData) {
                Pattern pattern = Pattern.compile("\\d+");

                // Create a matcher that will match the input string against the pattern
                Matcher matcher = pattern.matcher(data);

                // Find the first occurrence of a digit sequence in the string
                if (matcher.find()) {
                    // Extract the matched sequence (the digit in this case)
                    String digit = matcher.group();
                    cloudMap.put(digit, data);
                }
            }
            logger.info("-------------------------------------------------------------------");
            cloudMap.forEach((k ,v) -> logger.info(v));
            logger.info("-------------------------------------------------------------------");
            cloudData.clear();
            cloudMap.clear();
        }
        //logger.info(out);
        //String counterStr = counter.toString();
        //System.out.println(counter.toString());
        //outputCollector.emit(new Values(counterStr));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("printOut"));
    }
}
