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


public class TopKCloudyBolt extends BaseBasicBolt {
    BasicOutputCollector outputCollector;

    LossyCounter [] cloudCounter = new LossyCounter[5];

    ScheduledExecutorService scheduler;

    private int totalTuples;

    private void setupEpaCounters() {
        //yeah well this could be an array too
        for (int i = 0; i < cloudCounter.length; i++) {
            cloudCounter[i] = new LossyCounter();
        }
    }

    private void setUpPrintReportSchedule () {
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::printEpaCounters, 0, 1, TimeUnit.MINUTES);
    }

    private void printEpaCounters() {
        StringBuilder cloudCovOut = new StringBuilder();
        cloudCovOut.append("Per minute cloud coverage data : \n");
        for (int i = 0; i < cloudCounter.length; i++) {
            cloudCovOut.append("Top 5 states with cloud coverage level ").append(i + 1).append(" : ").append(cloudCounter[i].toString()).append("\n");
        }

        if (outputCollector != null) {
            outputCollector.emit(new Values(cloudCovOut));
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
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if (outputCollector == null) {
            outputCollector = basicOutputCollector;
        }
        totalTuples = tuple.getInteger(2);
        int processedTuples = tuple.getInteger(1);

        WeatherResponse response = (WeatherResponse) tuple.getValue(0);

        String state = response.getState();
        int cloudCover = response.getCloudCover();

        if (cloudCover >= 0 && cloudCover <= 19) {
            cloudCounter[0].accept(state);
        }
        else if (cloudCover >= 20 && cloudCover <= 39) {
            cloudCounter[1].accept(state);
        }
        else if (cloudCover >= 40 && cloudCover <= 59) {
            cloudCounter[2].accept(state);
        }
        else if (cloudCover >= 60 && cloudCover <= 79) {
            cloudCounter[3].accept(state);
        }
        else if (cloudCover >= 80 && cloudCover <= 100) {
            cloudCounter[4].accept(state);
        }

        //logger.info("Received AQI response - Zip: " + zip + ", Location: " + location + ", State "+ state + ", AQI: " + aqi);

        if (processedTuples % totalTuples == 0) {
            System.out.println("Processed a complete round of weather data for all zip codes");
//            setupEpaCounters();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("cloud_cover_data"));
    }

    @Override
    public void cleanup() {
        // Shutdown the scheduler when the bolt is being cleaned up
        scheduler.shutdown();
        super.cleanup();
    }
}
