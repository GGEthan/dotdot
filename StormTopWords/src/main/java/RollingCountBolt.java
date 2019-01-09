import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class RollingCountBolt extends BaseRichBolt{
	private OutputCollector collector;
	private final Map<Object, Integer> objToCounter = new HashMap<Object, Integer>();
	public RollingCountBolt() {
	}
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	public void execute(Tuple tuple) {
		Object obj = tuple.getValue(0);
		synchronized(objToCounter) {
			Integer curr = objToCounter.get(obj);
			if(curr == null) {
				curr = 0;
				objToCounter.put(obj, curr);
			}
			curr++;
			collector.emit(new Values(obj, curr));
			//collector.ack(tuple);
		}
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("obj", "count"));
	}

}
