
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class globalRankerBolt extends BaseBasicBolt{

	private static final int DEFAULT_COUNT = 10;
	private static List<Tuple> rankings = new ArrayList<Tuple>();
	private final int count;
	private static long lastTime;
	public globalRankerBolt() {
		this.count = DEFAULT_COUNT;
		lastTime = 0;
	}
	public globalRankerBolt(int topN) {
		if(topN < 1) {
			throw new IllegalArgumentException("topN must be >= 1 (you requested"+topN+")");
		}
		this.count = topN;
		lastTime = 0;
	
	}
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		List<Tuple> merging = (List)tuple.getValue(0);
		for(Tuple pair : merging)
		{
			Integer existingIndex = find(pair);
			if(existingIndex != null) {
				rankings.set(existingIndex, pair);
			}
			else {
				rankings.add(pair);
			}
		}
		Collections.sort(rankings, new Comparator<Tuple>() {
			public int compare(Tuple o1, Tuple o2) {
				return compareTuple(o1, o2);
			}
		});
		if(rankings.size() > count) {
			rankings.subList(count, rankings.size()).clear();
		}
		long currentTime = System.currentTimeMillis();
		if(lastTime ==0 || currentTime >= lastTime + 2000) {
			collector.emit(new Values(rankings));
			lastTime = currentTime;
		}

	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("list"));
	}
	private Integer find(Object tag) {
		int index = 0;
		for (Tuple i : rankings) {
			if(i.getValue(0)==tag) {
				return index;
			}
			index++;
		}
		return null;
	}
	private int compareTuple(Tuple tuple1, Tuple tuple2) {
		return (tuple2.getInteger(1)-tuple1.getInteger(1));
	}

}
