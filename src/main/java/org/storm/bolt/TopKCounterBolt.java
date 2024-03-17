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

public class TopKCounterBolt extends BaseBasicBolt {

    Logger logger;

    LossyCounter epa1Counter;
    LossyCounter epa2Counter;

    LossyCounter epa3Counter;

    LossyCounter epa4Counter;

    LossyCounter epa5Counter;

    ScheduledExecutorService scheduler;

    private int totalTuples;

    private Map <String, Long> stateCounterMap;

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
        if (stateCounterMap != null && !stateCounterMap.isEmpty()) {
            logger.info("Top 5 states with EPA 1: " + epa1Counter.toNormalizedString(stateCounterMap));
            logger.info("Top 5 states with EPA 2: " + epa2Counter.toNormalizedString(stateCounterMap));
            logger.info("Top 5 states with EPA 3: " + epa3Counter.toNormalizedString(stateCounterMap));
            logger.info("Top 5 states with EPA 4: " + epa4Counter.toNormalizedString(stateCounterMap));
            logger.info("Top 5 states with EPA 5: " + epa5Counter.toNormalizedString(stateCounterMap));
        }
        else {
            logger.info("Top 5 states with EPA 1: " + epa1Counter.toString());
            logger.info("Top 5 states with EPA 2: " + epa2Counter.toString());
            logger.info("Top 5 states with EPA 3: " + epa3Counter.toString());
            logger.info("Top 5 states with EPA 4: " + epa4Counter.toString());
            logger.info("Top 5 states with EPA 5: " + epa5Counter.toString());
        }

        //reset count now. start fresh for the next minute
        setupEpaCounters();
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
        //Utility stream data
        if (tuple.contains("source") && tuple.getStringByField("source").equals("UtilitySpout")) {
            stateCounterMap = (Map<String, Long>) tuple.getValueByField("stateCountMap");
            logger.info("Freq map in bolt "+ stateCounterMap.size());
        }
        //regular weather data
        else {
            totalTuples = tuple.getInteger(2);
            //List<WeatherResponse> aqiResponses = (List<WeatherResponse>) tuple.getValue(0);
            int processedTuples = tuple.getInteger(1);

            WeatherResponse response = (WeatherResponse) tuple.getValue(0);
            String zip = response.getZip();
            String location = response.getLocation();
            String state = response.getState();
            int aqi = response.getAqi();

            if (aqi == 1) {
                epa1Counter.accept(state);
            } else if (aqi == 2) {
                epa2Counter.accept(state);
            } else if (aqi == 3) {
                epa3Counter.accept(state);
            } else if (aqi == 4) {
                epa4Counter.accept(state);
            } else if (aqi == 5) {
                epa5Counter.accept(state);
            }

            //logger.info("Received AQI response - Zip: " + zip + ", Location: " + location + ", State "+ state + ", AQI: " + aqi);

            if (processedTuples % totalTuples == 0) {
                System.out.println("Processed a complete round of weather data for all zip codes");
                //setupEpaCounters();
            }
        }
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
