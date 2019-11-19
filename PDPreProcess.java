import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

private static enum PageCounter { PAGE_NUM }

public class PDPreProcess {

    public static class InputParser extends Mapper<Object, Text, IntWritable, MapWritable> {
        
        HashMap<IntWritable, MapWritable> map = new HashMap<IntWritable, MapWritable>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String nodes[] = value.toString().split(" ");
            IntWritable from = new IntWritable(Integer.parseInt(nodes[0]));
            IntWritable dest = new IntWritable(Integer.parseInt(nodes[1]));

            if(from == dest){
            }
            else{
                if(map.containsKey(from)){
                    MapWritable list = map.get(from);
                    list.put(dest, 0);
                }
                else{
                    MapWritable list = new MapWritable();
                    list.put(dest, 0);
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
            context.getCounter(PageCounter.PAGE_NUM).increment(map.size());
        }
    }

    public static class NodeReducer extends Reducer<IntWritable, MapWritable, IntWritable, PDNodeWritable> {
                
        public void reduce(IntWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable adjList = new MapWritable();
            for (MapWritable tuples : values){
                for (Map.Entry<Writable, Writable> tuple : tuples.entrySet()){
                    adjList.put(tuple.getKey(),tuple.getValue());
                }
            }
            Configuration conf = context.getConfiguration();
            int srcNodeID = Integer.parseInt(conf.get("srcNodeID"));
            IntWritable nodeID = key;
            BooleanWritable visited = new BooleanWritable(false);
            IntWritable distance = new IntWritable(-1);
            PDNodeWritable node;
            if(srcNodeID == key.get()){
                distance.set(0);
            }
            node = new PDNodeWritable(nodeID, visited, distance, adjList);
            context.write(key, node);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("srcNodeID", args[2]);
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "pre process");
        job.setJarByClass(PDPreProcess.class);
        job.setMapperClass(InputParser.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        //job.setCombinerClass(NodeReducer.class);
        job.setReducerClass(NodeReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PDNodeWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}