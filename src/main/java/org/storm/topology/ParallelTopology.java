package org.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.storm.bolt.TopKCloudyBolt;
import org.storm.bolt.TopKCloudyPrinterBolt;
import org.storm.bolt.TopKCounterBolt;
import org.storm.spout.CloudCoverageSpout;
import org.storm.spout.WeatherDataSpout;

import java.util.logging.Logger;

public class ParallelTopology {
    public static void main (String [] args) throws TException {
        Logger logger = Logger.getLogger(SerialTopology.class.getName());
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("CloudCoverageSpout", args != null && args.length > 1 ? new CloudCoverageSpout(args[1]): new CloudCoverageSpout());
        builder.setBolt("TopKCloudyBolt", new TopKCloudyBolt()).shuffleGrouping("CloudCoverageSpout");;
        builder.setBolt("TopKCloudyPrinterBolt", new TopKCloudyPrinterBolt()).shuffleGrouping("TopKCloudyBolt");;

        Config conf = new Config();
        //conf.setDebug(true);

        if (args != null && args.length > 0) {
            //logger.info("Args[1] is "+ args[1]);
            conf.put("filePath", args[1]);
            //conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            //conf.setMaxTaskParallelism(3);
            LocalCluster cluster = null;
            try {
                cluster = new LocalCluster();
            } catch (Exception var7) {
                throw new RuntimeException(var7);
            }

            cluster.submitTopology("cloud_cover", conf, builder.createTopology());
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException var5) {
                throw new RuntimeException(var5);
            }

            cluster.shutdown();
        }
    }

}
