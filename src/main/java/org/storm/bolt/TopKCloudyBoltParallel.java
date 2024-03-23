package org.storm.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
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

public class TopKCloudyBoltParallel extends BaseBasicBolt {
    BasicOutputCollector outputCollector;

    LossyCounter cloudCounter;

    ScheduledExecutorService scheduler;

    private int totalTuples;

    private void setupEpaCounters() {
        cloudCounter = new LossyCounter(0.01);
    }

    private void setUpPrintReportSchedule () {
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::printEpaCounters, 0, 1, TimeUnit.MINUTES);
    }

    private void printEpaCounters() {
        //System.out.println(cloudCounter.toString());
        if (outputCollector != null) {
            outputCollector.emit(new Values(cloudCounter, 1));
        }
        //reset count now. start fresh for the next minute
        setupEpaCounters();
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        setupEpaCounters();
        setUpPrintReportSchedule();
    }

    @Override
    public synchronized void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if (outputCollector == null) {
            outputCollector = basicOutputCollector;
        }
        totalTuples = tuple.getInteger(3);
        int processedTuples = tuple.getInteger(2);

        String state = (String) tuple.getValue(0);
        int cloudLevel = (int) tuple.getValue(1);

        cloudCounter.accept(state);
        cloudCounter.setDescriptor("Top 5 states with cloud level " + String.valueOf(cloudLevel));

        //System.out.println("Cloud level "+ cloudLevel);
        //logger.info("Received AQI response - Zip: " + zip + ", Location: " + location + ", State "+ state + ", AQI: " + aqi);

        if (processedTuples % totalTuples == 0) {
            System.out.println("Processed a complete round of weather data for all zip codes");
//            setupEpaCounters();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("cloud_cover_data", "printerBolt"));
    }

    @Override
    public void cleanup() {
        // Shutdown the scheduler when the bolt is being cleaned up
        scheduler.shutdown();
        super.cleanup();
    }
}
