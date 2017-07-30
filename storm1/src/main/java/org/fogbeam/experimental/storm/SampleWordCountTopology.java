
package org.fogbeam.experimental.storm;


import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


// a basic topology to play around with...
public class SampleWordCountTopology
{
	public static class WordCountBolt extends BaseBasicBolt
	{
		Map<String, Integer> counts = new HashMap<String, Integer>();


		@Override
		public void execute( Tuple tuple, BasicOutputCollector collector)
		{
			
			String word = tuple.getString( 0 );
			Integer count = counts.get( word );
			if( count == null )
				count = 0;
			count++;
			counts.put( word, count );
			
			System.out.println( "received word: " + tuple.getString( 0 ) + " : count = " + count );
			
			collector.emit( new Values( word, count ) );
		}

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("word", "count"));
	    }
	}

	public static void main( String[] args ) throws Exception
	{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout( "word", new OurWordSpout( 8000 ), 1 );
		builder.setBolt( "count1", new WordCountBolt(), 1 )
				.shuffleGrouping( "word" );
		Config conf = new Config();
		conf.setDebug( false );
		
		if( args != null && args.length > 0 )
		{
			conf.setNumWorkers( 3 );
			StormSubmitter.submitTopologyWithProgressBar( args[0], conf,
					builder.createTopology() );
		}
		else
		{
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology( "test", conf, builder.createTopology() );
			Utils.sleep( 600000 );
			cluster.killTopology( "test" );
			cluster.shutdown();
		}
	}
}
