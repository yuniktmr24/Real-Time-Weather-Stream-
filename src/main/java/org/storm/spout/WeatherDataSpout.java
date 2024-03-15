package org.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.domain.ZipCodeData;
import org.requests.AQIResponse;
import org.requests.WeatherAPIRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class WeatherDataSpout extends BaseRichSpout {
    SpoutOutputCollector outputCollector;
    private String filePath;

    public WeatherDataSpout(String path) {
        filePath = path;
    }

    public WeatherDataSpout() {
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;
    }

    /*
    @Override
    public void nextTuple() {
        //List<AQIResponse> realTimeWeatherData = WeatherAPIRequest.sendCurrentWeatherRequest(1);
        List <ZipCodeData> zip = WeatherAPIRequest.readZipCodes(filePath != null ? filePath : "src/main/resources/uszips_min.csv");

        System.out.println("Zip # "+ zip.size());
        for (ZipCodeData z: zip) {
            try {
                AQIResponse weatherData = WeatherAPIRequest.sendSingleWeatherRequest(z);
                if (weatherData != null) {
                    this.outputCollector.emit(new Values(weatherData));
                }
                Utils.sleep(5);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    */

    //async version
    @Override
    public void nextTuple() {
        List<ZipCodeData> zip = WeatherAPIRequest.readZipCodes(filePath != null ? filePath : "src/main/resources/uszips_min.csv");

        System.out.println("Zip # " + zip.size());
        AtomicInteger counter = new AtomicInteger(0);
        for (ZipCodeData z : zip) {
            WeatherAPIRequest.sendSingleWeatherRequestAsync(z)
                    .thenAccept(weatherData -> {
                        if (weatherData != null) {
                            outputCollector.emit(new Values(weatherData));
                        }
                        // sleep for a bit after 100 conns. to avoid to many files open
                        if (counter.incrementAndGet() % 100 == 0) {
                            Utils.sleep(5);
                            //outputCollector.emitEndOfStream();
                        }
                    })
                    .exceptionally(ex -> {
                        ex.printStackTrace();
                        return null;
                    });
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("weatherData"));
    }
}
