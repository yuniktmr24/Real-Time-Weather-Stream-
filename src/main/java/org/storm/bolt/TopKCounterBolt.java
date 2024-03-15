package org.storm.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.lossy.LossyCounter;
import org.requests.AQIResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class TopKCounterBolt extends BaseBasicBolt {

    Logger logger;

    LossyCounter epa1Counter;
    LossyCounter epa2Counter;

    LossyCounter epa3Counter;

    LossyCounter epa4Counter;

    LossyCounter epa5Counter;

    ScheduledExecutorService scheduler;

    private void setupEpaCounters() {
        //yeah well this could be an array too
        epa1Counter = new LossyCounter(0.01); //high freq, so more buckets than the rest
        epa2Counter = new LossyCounter();
        epa3Counter = new LossyCounter();
        epa4Counter = new LossyCounter();
        epa5Counter = new LossyCounter();
    }

    private void setUpPrintReportSchedule () {
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::printEpaCounters, 0, 1, TimeUnit.MINUTES);
    }

    private void printEpaCounters() {
        logger.info("Top 5 states with EPA 1: " + epa1Counter.toString());
        logger.info("Top 5 states with EPA 2: " + epa2Counter.toString());
        logger.info("Top 5 states with EPA 3: " + epa3Counter.toString());
        logger.info("Top 5 states with EPA 4: " + epa4Counter.toString());
        logger.info("Top 5 states with EPA 5: " + epa5Counter.toString());
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        // Initialize logger
        logger = Logger.getLogger(TopKCounterBolt.class.getName());
        try {
            // Create file handler
            FileHandler fh = new FileHandler("weather.log");
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
        //List<AQIResponse> aqiResponses = (List<AQIResponse>) tuple.getValue(0);

        AQIResponse response = (AQIResponse) tuple.getValue(0);
        String zip = response.getZip();
        String location = response.getLocation();
        String state = response.getState();
        int aqi = response.getAqi();

        if (aqi == 1) {
            epa1Counter.accept(state);
        }
        else if (aqi == 2) {
            epa2Counter.accept(state);
        }
        else if (aqi == 3) {
            epa3Counter.accept(state);
        }
        else if (aqi == 4) {
            epa4Counter.accept(state);
        }
        else if (aqi == 5) {
            epa5Counter.accept(state);
        }

        logger.info("Received AQI response - Zip: " + zip + ", Location: " + location + ", State "+ state + ", AQI: " + aqi);

//        // Process the list of AQIResponse objects
//        for (AQIResponse response : aqiResponses) {
//            // Process each AQIResponse object
//            String zip = response.getZip();
//            String location = response.getLocation();
//            int aqi = response.getAqi();
//
//            // Do something with the AQIResponse object
//            //System.out.println("Received AQI response - Zip: " + zip + ", Location: " + location + ", AQI: " + aqi);
//            logger.info("Received AQI response - Zip: " + zip + ", Location: " + location + ", AQI: " + aqi);
//        }
    }

    private void emitRankings (BasicOutputCollector collector) {
        //todo lossy counting alg to get rankings
        //collector.emit();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("aqi"));
    }

    @Override
    public void cleanup() {
        // Shutdown the scheduler when the bolt is being cleaned up
        scheduler.shutdown();
        super.cleanup();
    }
}
