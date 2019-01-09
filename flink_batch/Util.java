package DSPPCode.flink_batch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Util {

    /**
     * 将 String 转换为 Map.
     * 如 { 0 => { 3 => 10 } } 表示从节点0到节点3有一条长为10的单向边.
     */
    static DataSet<Map<Integer, Map<Integer, Integer>>> initGraph(DataSet<String> data) {
        return data
                .map(new MapFunction<String, Tuple2<Integer, Map<Integer, Integer>>>() {
                    public Tuple2<Integer, Map<Integer, Integer>> map(String s) {
                        String[] strs = s.split(";");
                        Map<Integer, Integer> distMap = new HashMap<Integer, Integer>();
                        for (int i = 1; i < strs.length; i++) {
                            String[] dist = strs[i].split(",");
                            distMap.put(Integer.parseInt(dist[0]), Integer.parseInt(dist[1]));
                        }
                        return new Tuple2<Integer, Map<Integer, Integer>>(Integer.parseInt(strs[0]), distMap);
                    }
                })
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer, Map<Integer, Integer>>,
                        Map<Integer, Map<Integer, Integer>>>() {
                    public void reduce(Iterable<Tuple2<Integer, Map<Integer, Integer>>> iterable,
                                       Collector<Map<Integer, Map<Integer, Integer>>> collector) {
                        Map<Integer, Map<Integer, Integer>> graph = new HashMap<Integer, Map<Integer, Integer>>();
                        for (Tuple2<Integer, Map<Integer, Integer>> distMap : iterable) {
                            graph.put(distMap.f0, distMap.f1);
                        }
                        collector.collect(graph);
                    }
                });
    }

    /**
     * 初始化从节点0到所有节点的最短距离.
     * 起点到自己的距离为0, 到其他节点的距离为Integer.MAX_VALUE.
     * 如总共2个节点, 则返回 { {0, 0}, {1, Integer.MAX_VALUE} } .
     */
    static DataSet<Tuple2<Integer, Integer>> initShortestDist(int nodeNum) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, Integer>> shortestDist = new ArrayList<Tuple2<Integer, Integer>>();
        for (int i = 0; i < nodeNum; i++) {
            if (i != getSourceNode()) {
                shortestDist.add(new Tuple2<Integer, Integer>(i, Integer.MAX_VALUE));
            } else {
                shortestDist.add(new Tuple2<Integer, Integer>(i, 0));
            }
        }
        return env.fromCollection(shortestDist);
    }

    /**
     * 默认以节点0为起点
     */
    private static int getSourceNode() {
        return 0;
    }

}
