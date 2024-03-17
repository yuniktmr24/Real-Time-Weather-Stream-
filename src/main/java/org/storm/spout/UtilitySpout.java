package org.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.domain.DomainUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UtilitySpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Map<String, Long> stateCounts = new HashMap<>();

    boolean emittedOnce = false;
    @Override
    public void open(Map<String, Object> conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        String filePath = (String) conf.get("filePath");
        System.out.println("In Utility Spout : "+ filePath);

        try {
            stateCounts = DomainUtils.getStateCounts(filePath);
            System.out.println("Collected frequency data for "+ stateCounts.size() + " states");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        while (!emittedOnce) {
            // Emit the state count map as a single tuple
            collector.emit(new Values(stateCounts, "UtilitySpout"));
            // emit once
            emittedOnce = true;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("stateCountMap", "source"));
    }
}
