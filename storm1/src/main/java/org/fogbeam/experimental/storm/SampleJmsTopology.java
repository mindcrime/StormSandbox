
package org.fogbeam.experimental.storm;


import javax.jms.Session;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.jms.spout.JmsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;


public class SampleJmsTopology
{
	public static final String JMS_QUEUE_SPOUT = "JMS_QUEUE_SPOUT";

	
	public static void main( String[] args ) throws Exception
	{
		// JMS Queue Provider
		JmsProvider jmsQueueProvider = new SpringJmsProvider(
				"jms-activemq.xml", "jmsConnectionFactory",
				"notificationQueue" );
		
		
		// JMS Producer
        JmsTupleProducer producer = new JsonTupleProducer();
		
		
		// JMS Queue Spout
		JmsSpout queueSpout = new JmsSpout();
		queueSpout.setJmsProvider( jmsQueueProvider );
		queueSpout.setJmsTupleProducer(producer);
		queueSpout.setJmsAcknowledgeMode( Session.CLIENT_ACKNOWLEDGE );
		queueSpout.setDistributed( true ); // allow multiple instances
		
		TopologyBuilder builder = new TopologyBuilder();
		
		// spout with 5 parallel instances
		builder.setSpout( JMS_QUEUE_SPOUT, queueSpout, 5 );
		
		// bolt, subscribes to jms spout, anchors on tuples, and auto-acks
		builder.setBolt("messageProcessorBolt", new GenericBolt("processorBolt1", true, true, null), 3).shuffleGrouping(JMS_QUEUE_SPOUT);
		
		
		Config conf = new Config();
		
		if( args.length > 0 )
		{
			conf.setNumWorkers( 3 );
			StormSubmitter.submitTopology( args[0], conf, builder.createTopology() );
		}
		else
		{
			conf.setDebug( false );
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology( "storm-jms-example", conf, builder.createTopology() );
			Utils.sleep( 1000000 );
			cluster.killTopology( "storm-jms-example" );
			cluster.shutdown();
		}
	}
}
