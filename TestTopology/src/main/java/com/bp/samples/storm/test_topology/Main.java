package com.bp.samples.storm.test_topology;

import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.bp.samples.storm.test_topoology.spouts.MerchantFeederSpout;

public class Main {

	/**
	 * @param args 1
	 */
	public static void main(String[] args) throws InterruptedException {
        //Configuration
		Config conf = new Config();
		conf.put(AppConsts.MERCHANTS_FILE, args[0]);
		conf.put(AppConsts.BCD_START, args[1]);
		conf.put(AppConsts.BCD_END, args[2]);
		//System.out.println("user.dir:" + System.getProperty("user.dir"));
		//List<String> list = MerchantFeederSpout.getMerchantAccNoList(conf.get(AppConsts.MERCHANTS_FILE).toString());
		//for (String l:list)
		//	System.out.println(l);
		//System.exit(0);
		conf.setDebug(false);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		//Creating the topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("Merchant-Feeder",new MerchantFeederSpout());
		
		
		LocalCluster cluster = new LocalCluster();
		//cluster.submitTopology("Count-Word-Toplogy-With-Refresh-Cache", conf, builder.createTopology());
		Thread.sleep(5000);
		cluster.shutdown();
	}

}
