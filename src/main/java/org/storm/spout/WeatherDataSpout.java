package org.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.domain.DomainUtils;
import org.domain.ZipCodeData;
import org.requests.WeatherAPIRequest;
import org.storm.bolt.TopKCloudyBolt;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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

    public void prepare(Map<String, Object> topoConf, TopologyContext context) throws IOException {
        DomainUtils.getStateCounts(filePath != null ? filePath : "src/main/resources/uszips_min.csv");
    }

    /* Serial version
    @Override
    public void nextTuple() {
        //List<WeatherResponse> realTimeWeatherData = WeatherAPIRequest.sendCurrentWeatherRequest(1);
        List <ZipCodeData> zip = WeatherAPIRequest.readZipCodes(filePath != null ? filePath : "src/main/resources/uszips_min.csv");

        System.out.println("Zip # "+ zip.size());
        for (ZipCodeData z: zip) {
            try {
                WeatherResponse weatherData = WeatherAPIRequest.sendSingleWeatherRequest(z);
                if (weatherData != null) {
                    this.outputCollector.emit(new Values(weatherData));
                }
                DomainUtils.sleep(5);
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
        AtomicInteger processedCounter = new AtomicInteger(0);
        AtomicInteger connectionsCreatedCounter = new AtomicInteger(0);
        for (ZipCodeData z : zip) {
            WeatherAPIRequest.sendSingleWeatherRequestAsync(z)
                    .thenAccept(weatherData -> {
                        processedCounter.incrementAndGet();
                        if (weatherData != null) {
                            outputCollector.emit(new Values(weatherData, processedCounter.get(), zip.size()));
                        }
                    })
                    .exceptionally(ex -> {
                        processedCounter.incrementAndGet();
                        ex.printStackTrace();
                        return null;
                    });
            // sleep for a bit after 100 conns to avoid too many files open. Throttling
            if (connectionsCreatedCounter.incrementAndGet() % 80 == 0) {
                Utils.sleep(100);
                //outputCollector.emitEndOfStream();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("weatherData", "counter", "totalTuples"));
    }
}
