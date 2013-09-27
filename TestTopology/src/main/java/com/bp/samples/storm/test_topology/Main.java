package com.bp.samples.storm.test_topology;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.bp.samples.storm.test_topology.bolts.MerchantProcessorBolt;
import com.bp.samples.storm.test_topology.spouts.MerchantFeederSpout;

/**
 * The main method of this class creates the storm topology and submits it!
 * 
 * @author bpirvali
 *
 * @version 1.0
 * 
 */

/*
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader",new WordReader());
		builder.setSpout("signals-spout",new SignalsSpout());
		builder.setBolt("word-normalizer", new WordNormalizer())
			.shuffleGrouping("word-reader");
		
		builder.setBolt("word-counter", new WordCounter(),2)
			.fieldsGrouping("word-normalizer",new Fields("word"))
			.allGrouping("signals-spout","signals");

		
        //Configuration
		Config conf = new Config();
		conf.put("wordsFile", args[0]);
		conf.setDebug(true);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Count-Word-Toplogy-With-Refresh-Cache", conf, builder.createTopology());
		Thread.sleep(5000);
		cluster.shutdown();
 */
public class Main {
	static Logger logger = Logger.getRootLogger();
	public static void main(String[] args) throws InterruptedException {
	      logger.setLevel((Level) Level.WARN);
	      logger.debug("Here is some DEBUG");
	      logger.info("Here is some INFO");
	      logger.warn("Here is some WARN");
	      logger.error("Here is some ERROR");
	      logger.fatal("Here is some FATAL");
	      
        //Configuration
		Config conf = new Config();
		
		// parallelize the spout
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100);
		
		// enable reliable messaging
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 5);
		
		// set debug to false
		conf.setDebug(false);
		
		// Customize config with my own data
		conf.put(AppConsts.MERCHANTS_FILE, args[0]);
		conf.put(AppConsts.BCD_START, args[1]);
		conf.put(AppConsts.BCD_END, args[2]);
		
		// set no of workers
		conf.setNumWorkers(3);
		
		// build the topology
		TopologyBuilder builder = new TopologyBuilder();
		
		// set the parallelism hint to two --> 1 spout / worker process
		builder.setSpout("MerchantFeederSpout", new MerchantFeederSpout(), 3);
		
		// set the parallelism hint to four --> 2 bolts / worker process
		builder.setBolt("MerchantProcessorBolt", new MerchantProcessorBolt(), 6).shuffleGrouping("MerchantFeederSpout");		
		
		//System.out.println("user.dir:" + System.getProperty("user.dir"));
		//List<String> list = MerchantFeederSpout.getMerchantAccNoList(conf.get(AppConsts.MERCHANTS_FILE).toString());
		//for (String l:list)
		//	System.out.println(l);
		//System.exit(0);
		
		// create cluster 
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Merchant-Batch-Processing-Topology", conf, builder.createTopology());
		System.out.println("Sleeping for 50 secs...");
		Thread.sleep(50000);
		System.out.println("Shuting down the cluster...");
		cluster.shutdown();
	}

}
