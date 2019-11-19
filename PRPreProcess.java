import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PRPreProcess {

    public static enum NodeCounter { COUNT };

    public static class InputParser extends Mapper<Object, Text, IntWritable, MapWritable> {
        
        HashMap<IntWritable, MapWritable> map = new HashMap<IntWritable, MapWritable>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();            
            String nodes[] = value.toString().split(" ");
            IntWritable from = new IntWritable(Integer.parseInt(nodes[0]));
            IntWritable dest = new IntWritable(Integer.parseInt(nodes[1]));
            IntWritable weight = new IntWritable(1);

            if(from.get() == dest.get()){
                if(map.containsKey(from)){
                    // do nothing
                }
                else{
                    map.put(from, new MapWritable());
                }
            }
            else{
                if(map.containsKey(from)){
                    MapWritable list = map.get(from);
                    list.put(dest, weight);
                }
                else{
                    MapWritable list = new MapWritable();
                    list.put(dest,weight);
                    map.put(from, list);
                }
            }

            if(map.containsKey(dest)){
                // do nothing
            }
            else{
                map.put(dest, new MapWritable());
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<IntWritable, MapWritable> node : map.entrySet()){
                context.write(node.getKey(), node.getValue());
            }
        }
    }

    public static class NodeReducer extends Reducer<IntWritable, MapWritable, IntWritable, PRNodeWritable> {
                
        public void reduce(IntWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable adjList = new MapWritable();
            for (MapWritable tuples : values){
                for (Map.Entry<Writable, Writable> tuple : tuples.entrySet()){
                    adjList.put(tuple.getKey(),tuple.getValue());
                }
            }
            DoubleWritable prValue = new DoubleWritable();
            IntWritable childNum = new IntWritable(adjList.size());
            PRNodeWritable node = new PRNodeWritable(key, prValue, childNum, adjList);
            context.getCounter(NodeCounter.COUNT).increment(1);
            context.write(key, node);
        }
    }

}