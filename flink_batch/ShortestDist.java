package DSPPCode.flink_batch;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 以节点0位起点, 计算单源最短距离.
 * -
 * 输入:
 * 每个 String 代表以某个节点为起点的所有单向边信息, String 内由 ";" 隔开, 第一个代表节点ID, 之后每个代表一条单向边.
 * 如 0;3,10 表示从节点0到节点3有一条长为10的单向边.
 * (已提供 Util.initGraph(data) 可将输入解析为 Map)
 * -
 * 输出:
 * 每个 Tuple2<Integer, Integer> 代表从节点0到某一点的最短距离.
 * 如 { 3, 5 } 代表从节点0到节点3的最短距离为5.
 */
public class ShortestDist {

    public static DataSet<Tuple2<Integer, Integer>> run(DataSet<String> data) throws Exception {
        int nodeNum = (int) data.count();
        DataSet<Map<Integer, Map<Integer, Integer>>> graph = Util.initGraph(data);
        /*(node, shortest distance node)*/
        DataSet<Tuple2<Integer, Integer>> shortestDist = Util.initShortestDist(nodeNum);
        IterativeDataSet<Tuple2<Integer, Integer>> initial = shortestDist.iterate(nodeNum - 1);

        DataSet<Tuple2<Integer, Integer>> iteration = initial
                .flatMap(new FlatMapper()).withBroadcastSet(graph, "graph")
                .groupBy(0)
                .reduce(new Reducer());

        return initial.closeWith(iteration);
    }

    public static class FlatMapper extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        private Map<Integer, Map<Integer, Integer>> graph;

        @Override
        public void open(Configuration parameters) {
            List<Map<Integer, Map<Integer, Integer>>> collection = getRuntimeContext()
                    .getBroadcastVariable("graph");
            graph = collection.get(0);
        }

        public void flatMap(Tuple2<Integer, Integer> shortestDist, Collector<Tuple2<Integer, Integer>> collector) {
            //TODO 请完成该函数
        	collector.collect(shortestDist);
        	if(shortestDist.f1 == Integer.MAX_VALUE)
        		return;

        	for(Integer iter : graph.keySet())
        	{
        		if(iter == shortestDist.f0)
        		{
        			Map<Integer,Integer> temp = graph.get(iter);
        			for(Integer iter1 : temp.keySet())
        			{
        				Integer temp1 = temp.get(iter1);
        				int newDistance = shortestDist.f1 + temp1;
        				collector.collect(new Tuple2<Integer, Integer>(iter1,newDistance));
        			}
        			break;
        		}
        			
        	}

        }

    }

    public static class Reducer implements ReduceFunction<Tuple2<Integer, Integer>> {

        public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
            //TODO 请完成该函数
        	if(t2 == null)
        		return t1;
        	if(t1.f1 < t2.f1)
        		return t1;
        	return t2;

        }

    }

}
