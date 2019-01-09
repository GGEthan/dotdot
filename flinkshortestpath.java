import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class ShortesetPath {
	private final static int MAXITERATION = 10;
	private final static String READPATHV = "/home/hadoop/graph_algo/shortestpath/flink/vertex";
	private final static String READPATHE = "/home/hadoop/graph_algo/shortestpath/flink/edge";
	private final static String WRITEPATH = "/home/hadoop/graph_algo/shortestpath/flink";
	private final static String SourceNode = "A";
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<String,Tuple2<String,String>>> vertexWithDistance = env.readTextFile(READPATHV).map(new MapFunction<String, Tuple2<String,Tuple2<String,String>>>() {
			public Tuple2<String,Tuple2<String,String>> map(String value) {
				String initWeight;
				if(value.equals(SourceNode)) {
					initWeight = "0";
				}
				else
					initWeight = "inf";
				Tuple2<String,String> t = new Tuple2<String,String>(initWeight,value);
				return new Tuple2<String, Tuple2<String,String>>(value,t);
			}
		});
		DataSet<Tuple2<String,Tuple2<String,String>>> edgesWithWeight = env.readTextFile(READPATHE).map(new MapFunction<String,Tuple2<String,Tuple2<String,String>>>() {
			public Tuple2<String,Tuple2<String,String>> map(String value) {
				String[] strs = value.split(" |\t");
				Tuple2<String,String> t = new Tuple2<String,String>(strs[1],strs[2]);
				return new Tuple2<String,Tuple2<String,String>>(strs[0],t);
			}
		});
		int i = 0;
		while(i < MAXITERATION) {
			DataSet<Tuple2<String,Tuple2<String,String>>> tempRes = 
					vertexWithDistance.join(edgesWithWeight).where(0).equalTo(0)
														.with(new distanceUpdate())
														.groupBy(0)
														.reduce(new ReduceFunction<Tuple2<String,Tuple2<String,String>>>(){
								public Tuple2<String,Tuple2<String,String>> reduce (Tuple2<String,Tuple2<String,String>> r1, Tuple2<String,Tuple2<String,String>> r2){

									int min = Integer.parseInt(r1.f1.f0);
									if(min > Integer.parseInt(r2.f1.f0))
									{
										min = Integer.parseInt(r2.f1.f0);
										return r2;
									}	
									return r1;
								}
								});
					
			vertexWithDistance = vertexWithDistance.union(tempRes).groupBy(0).reduceGroup(new minDistanceFilter());
			vertexWithDistance.print();
			System.out.println("*******************************************");
			tempRes.print();
			System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
			if(vertexWithDistance == tempRes)
				break;
			i++;
		}
		vertexWithDistance.writeAsText(WRITEPATH);
		vertexWithDistance.print();
	}
	public static final class distanceUpdate implements FlatJoinFunction<Tuple2<String,Tuple2<String,String>>,Tuple2<String,Tuple2<String,String>>,Tuple2<String,Tuple2<String,String>>> {
		public void join(Tuple2<String,Tuple2<String,String>> v1, Tuple2<String,Tuple2<String,String>> v2,Collector<Tuple2<String,Tuple2<String,String>>> out) {
			int newDistance;
			if(!v1.f1.f0.equals("inf"))
			{
				newDistance = Integer.parseInt(v1.f1.f0) + Integer.parseInt(v2.f1.f1); 
				Tuple2<String,String> t = new Tuple2<String,String>(Integer.toString(newDistance),v1.f0);
				Tuple2 res = new Tuple2<String,Tuple2<String,String>> (v2.f1.f0,t);
				out.collect(res);
			}
		}
	}
	public static final class minDistanceFilter implements GroupReduceFunction<Tuple2<String,Tuple2<String,String>>,Tuple2<String,Tuple2<String,String>>> {
		public void reduce(Iterable<Tuple2<String,Tuple2<String,String>>> value, Collector<Tuple2<String,Tuple2<String,String>>> out) {
			List<Tuple2<String,Tuple2<String,String>>> temp = new ArrayList<Tuple2<String,Tuple2<String,String>>>();
			Iterator<Tuple2<String, Tuple2<String, String>>> ter = value.iterator();
			while(ter.hasNext())
			{
				temp.add(ter.next());
			}
			if(temp.size() == 1) 
			{
				out.collect(temp.get(0));
			}
			else
			{
				String min = temp.get(0).f1.f0;
				int min_index = 0;
				for(int i = 1;i<temp.size();i++)
				{
					Tuple2<String,Tuple2<String,String>> t = temp.get(i);
					if(t.f1.f0.equals("inf"));
					else if(min.equals("inf"))
					{
						min = t.f1.f0;
						min_index = i;						
					}
					else if(Integer.parseInt(t.f1.f0) < Integer.parseInt(min))
					{
							min = t.f1.f0;
							min_index = i;
					}
				}
				out.collect(temp.get(min_index));
			}
		}
	}
	
}
