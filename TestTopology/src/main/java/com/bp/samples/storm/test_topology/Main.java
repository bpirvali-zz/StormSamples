package com.bp.samples.storm.test_topology;

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
	public static void main(String[] args) throws InterruptedException {
        //Configuration
		Config conf = new Config();
		conf.put(AppConsts.MERCHANTS_FILE, args[0]);
		conf.put(AppConsts.BCD_START, args[1]);
		conf.put(AppConsts.BCD_END, args[2]);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100);
		conf.setDebug(false);
        //Topology run
		
		// build the topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("MerchantFeederSpout", new MerchantFeederSpout(), 1);
		builder.setBolt("MerchantProcessorBolt", new MerchantProcessorBolt(), 1).shuffleGrouping("MerchantFeederSpout");		
		
		//System.out.println("user.dir:" + System.getProperty("user.dir"));
		//List<String> list = MerchantFeederSpout.getMerchantAccNoList(conf.get(AppConsts.MERCHANTS_FILE).toString());
		//for (String l:list)
		//	System.out.println(l);
		//System.exit(0);
		
		// create cluster 
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Merchant-Batch-Processing-Topology", conf, builder.createTopology());
		System.out.println("Sleeping for 5 secs...");
		Thread.sleep(5000);
		System.out.println("Shuting down the cluster...");
		cluster.shutdown();
	}

}
