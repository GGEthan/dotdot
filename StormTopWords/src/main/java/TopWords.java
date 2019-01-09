import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopWords {
	private static final int TOP_N = 5;
	
	private final TopologyBuilder builder;
	//private final String topologyName;
	private final Config topologyConfig;
	private final String topologyName;

	public TopWords() throws InterruptedException {
		builder = new TopologyBuilder();
		topologyName = "RollingCounts";
		topologyConfig = createTopologyConfiguration();
		
		TopologyModel();
		
	}
	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(true);
		return conf;
	}
	
	private void TopologyModel() throws InterruptedException {
		String spoutId = "randomGenerator";
		String counterId = "counter";
		String localRankerId = "localRanker";
		String globalRankerId = "globalRanker";
		builder.setSpout(spoutId, new TestWordSpout(), 5);
		builder.setBolt(counterId, new RollingCountBolt(), 4)
						.fieldsGrouping(spoutId, new Fields("word"));
		builder.setBolt(localRankerId, new localRankerBolt(TOP_N), 4).fieldsGrouping(counterId, new Fields("obj"));
		builder.setBolt(globalRankerId, new globalRankerBolt(TOP_N))
						.globalGrouping(localRankerId);
	}
	public void run() throws InterruptedException {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, topologyConfig,  builder.createTopology());
		Thread.sleep(60 * 1000);
		cluster.killTopology(topologyName);
		cluster.shutdown();
	}
	public static void main(String[] args) throws Exception {
		new TopWords().run();
		
	}
}
