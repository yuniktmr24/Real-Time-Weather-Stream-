package org.storm.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.lossy.LossyCounter;
import org.requests.WeatherResponse;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class TopKCloudyBolt extends BaseBasicBolt {
    Logger logger;

    LossyCounter cloud1Counter;
    LossyCounter cloud2Counter;

    LossyCounter cloud3Counter;

    LossyCounter cloud4Counter;

    LossyCounter cloud5Counter;

    ScheduledExecutorService scheduler;

    private int totalTuples;

    private void setupEpaCounters() {
        //yeah well this could be an array too
        cloud1Counter = new LossyCounter();
        cloud2Counter = new LossyCounter();
        cloud3Counter = new LossyCounter();
        cloud4Counter = new LossyCounter();
        cloud5Counter = new LossyCounter();
    }

    private void setUpPrintReportSchedule () {
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::printEpaCounters, 0, 1, TimeUnit.MINUTES);
    }

    private void printEpaCounters() {
        logger.info("Top 5 states with cloud coverage level 1: " + cloud1Counter.toString());
        logger.info("Top 5 states with cloud coverage level 2: " + cloud2Counter.toString());
        logger.info("Top 5 states with cloud coverage level 3: " + cloud3Counter.toString());
        logger.info("Top 5 states with cloud coverage level 4: " + cloud4Counter.toString());
        logger.info("Top 5 states with cloud coverage level 5: " + cloud5Counter.toString());

        //reset count now. start fresh for the next minute
        setupEpaCounters();
    }

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

            setupEpaCounters();
            setUpPrintReportSchedule();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        totalTuples = tuple.getInteger(2);
        int processedTuples = tuple.getInteger(1);

        WeatherResponse response = (WeatherResponse) tuple.getValue(0);

        String state = response.getState();
        int cloudCover = response.getCloudCover();

        if (cloudCover >= 0 && cloudCover <= 19) {
            cloud1Counter.accept(state);
        }
        else if (cloudCover >= 20 && cloudCover <= 39) {
            cloud2Counter.accept(state);
        }
        else if (cloudCover >= 40 && cloudCover <= 59) {
            cloud3Counter.accept(state);
        }
        else if (cloudCover >= 60 && cloudCover <= 79) {
            cloud4Counter.accept(state);
        }
        else if (cloudCover >= 80 && cloudCover <= 100) {
            cloud5Counter.accept(state);
        }

        //logger.info("Received AQI response - Zip: " + zip + ", Location: " + location + ", State "+ state + ", AQI: " + aqi);

        if (processedTuples % totalTuples == 0) {
            System.out.println("Processed a complete round of weather data for all zip codes");
//            setupEpaCounters();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("cloud_cover"));
    }

    @Override
    public void cleanup() {
        // Shutdown the scheduler when the bolt is being cleaned up
        scheduler.shutdown();
        super.cleanup();
    }
}
