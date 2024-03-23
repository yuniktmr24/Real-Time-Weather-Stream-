package org.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.domain.ZipCodeData;
import org.requests.WeatherAPIRequest;
import org.requests.WeatherResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudCoverageSpout extends BaseRichSpout {
    SpoutOutputCollector outputCollector;
    private String filePath;

    public CloudCoverageSpout(String filePath) {
        this.filePath = filePath;
    }

    public CloudCoverageSpout() {
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        List<ZipCodeData> zip = WeatherAPIRequest.readZipCodes(filePath != null ? filePath : "src/main/resources/uszips_min.csv");

        System.out.println("Zip # " + zip.size());
        AtomicInteger processedCounter = new AtomicInteger(0);
        AtomicInteger connectionsCreatedCounter = new AtomicInteger(0);
        for (ZipCodeData z : zip) {
            WeatherAPIRequest.sendWeatherForecastRequestAsync(z)
                    .thenAccept(weatherDataList -> {
                        processedCounter.incrementAndGet();
                        for (WeatherResponse weatherData : weatherDataList) {
                            if (weatherData != null) {
                                int cloudLevel = 0;
                                int cloudCover = weatherData.getCloudCover();
                                if (cloudCover >= 0 && cloudCover <= 19) {
                                    cloudLevel = 1;
                                } else if (cloudCover >= 20 && cloudCover <= 39) {
                                    cloudLevel = 2;
                                } else if (cloudCover >= 40 && cloudCover <= 59) {
                                    cloudLevel = 3;
                                } else if (cloudCover >= 60 && cloudCover <= 79) {
                                    cloudLevel = 4;
                                } else if (cloudCover >= 80 && cloudCover <= 100) {
                                    cloudLevel = 5;
                                }
                                outputCollector.emit(new Values(weatherData.getState(), cloudLevel, processedCounter.get(), zip.size()));
                            }
                        }
                    });
//                    .exceptionally(ex -> {
//                        processedCounter.incrementAndGet();
//                       // ex.printStackTrace();
//                        return null;
//                    });
            // sleep for a bit after 100 conns to avoid too many files open. Throttling
            if (connectionsCreatedCounter.incrementAndGet() % 80 == 0) {
                Utils.sleep(100);
                //outputCollector.emitEndOfStream();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("weatherDataState", "cloudLevel", "counter", "totalTuples"));
    }
}
